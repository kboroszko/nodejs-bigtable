import {Duplex} from 'stream';
import {ProtobufReaderTransformer} from './protobufreadertransformer';
import {AbortableDuplex} from '..';
import {ServiceError} from 'google-gax';
import {
  DEFAULT_BACKOFF_SETTINGS,
  DEFAULT_RETRY_COUNT,
  IGNORED_STATUS_CODES,
  RETRYABLE_STATUS_CODES,
  isRstStreamError,
} from '../utils/retry-options';
import {getNextDelay} from '../table';
import {BackoffSettings} from 'google-gax/build/src/gax';
import 'abort-controller/polyfill';

/**
 * Creates a DrainGuard and returns a promise which is resolved:
 *     to `true` when the DrainGuard is handled
 *     to `false` when the abortController is aborted
 *
 * @param {Duplex} stream to which we write the drainGuard object.
 * @param {AbortController} abortController which will resolve
 *     the returned promise to false when aborted.
 * @returns a promise which resolves to `true` when the DrainGuard is handled or
 *     to `false` when the abortController is aborted
 */
function onWritableDrain(
  stream: Duplex,
  abortController: AbortController
): Promise<boolean> {
  // We detect the drain manually because stream's 'drain' event won't be emitted
  // if the writable buffer was only partialy filled.
  return new Promise<boolean>(resolve => {
    abortController.signal.addEventListener('abort', () => resolve(false));
    stream.write(
      ProtobufReaderTransformer.createDrainGuard(() => resolve(true))
    );
  });
}

/**
 * Handles retries on the callerStream.
 * When a retryable error happens, a retry is made. The newRequest function is
 * called, and the created streams are piped to the callerStream. The old ones
 * are aborted and discarded.
 *
 * This function uses exponential backoff with randomized delay for retries.
 *
 * @param {Function} newRequest
 *     This function is expected to return a pair of streams.
 *     These streams will be connected by event listeners to the callerStream
 *     to ensure proper retry and cancelation handling.
 *     The bigtableStream shall be aborted when caller invokes end.
 *     The valuesStream will be piped to the callerStream.
 *     bigtableStream and valuesStream may or may not be the same object.
 * @param {Duplex} callerStream The stream that caller gets.
 * @param {Function} onCallerCancelled function called when the caller invokes
 *   `end` on the callerStream.
 * @param {BackoffSettings | undefined} backoffSettings
 */
export function setupRetries(
  newRequest: () => {
    bigtableStream: AbortableDuplex;
    valuesStream: Duplex;
  },
  callerStream: Duplex,
  onCallerCancelled: () => void,
  backoffSettings?: BackoffSettings
): void {
  let numConsecutiveErrors = 0;
  let activeRequestStream: AbortableDuplex | null;
  let retryTimer: NodeJS.Timeout | null;
  const maxRetries: number = backoffSettings?.maxRetries || DEFAULT_RETRY_COUNT;

  // The caller should be able to call callerStream.end() to stop receiving
  // more rows and cancel the stream prematurely. However this has a side effect
  // the 'end' event will be emitted.
  // We don't want that, because it also gets emitted if the stream ended
  // normally. To tell these two situations apart, we'll overwrite the end
  // function, but save the "original" end() function which will be called
  // on valueStream.on('end').
  const originalEnd = callerStream.end.bind(callerStream);

  // We need to explicitly connect the `originalEnd`
  // when piping and unpiping callerStream
  const rowStreamPipe = (valueStream: Duplex, callerStream: Duplex) => {
    valueStream.pipe(callerStream, {end: false});
    valueStream.on('end', originalEnd);
  };
  const rowStreamUnpipe = (valueStream: Duplex, callerStream: Duplex) => {
    valueStream?.unpipe(callerStream);
    valueStream?.removeListener('end', originalEnd);
  };

  const makeNewRequest = (): void => {
    const abortController = new AbortController();
    // If we makeNewRequest then we know that the last retryTimer is either null
    // or is expired. We set it to null to make sure we will not try to clear it
    // if the caller cancels between now and when the new timer is created.
    retryTimer = null;

    // This callback is called if the caller cancelled the request.
    abortController.signal.addEventListener('abort', () => {
      if (retryTimer) {
        clearTimeout(retryTimer);
      }
      if (activeRequestStream) {
        activeRequestStream.abort();
      }
      onCallerCancelled();
    });

    const streams = newRequest();
    activeRequestStream = streams.bigtableStream;
    const valueStream = streams.valuesStream;

    valueStream
      .on('error', (error: ServiceError) => {
        rowStreamUnpipe(valueStream, callerStream);
        activeRequestStream = null;
        if (IGNORED_STATUS_CODES.has(error.code)) {
          // We ignore the `cancelled` "error", since we are the ones who cause
          // it when the caller calls `.abort()`.
          callerStream.end();
          return;
        }
        numConsecutiveErrors++;
        if (
          numConsecutiveErrors <= maxRetries &&
          (RETRYABLE_STATUS_CODES.has(error.code) || isRstStreamError(error))
        ) {
          const backOffSettings = backoffSettings || DEFAULT_BACKOFF_SETTINGS;
          const nextRetryDelay = getNextDelay(
            numConsecutiveErrors,
            backOffSettings
          );
          // We want to make a new request only when all requests already written to the Reader by our
          // previous active request stream were processed.
          //
          // Writable streams keep a buffer of objects to
          // process (in case of a Transform processing means calling _transform() method). Readable streams
          // keep a buffer of objects to be read by downstream processors. Transforms are both Writable and
          // Readable, thus they have one buffer for parameters to, and one buffer for results of
          // _transform() method.
          // Objects can end up in a writeable's buffer if they are written after previous write call
          // returned false but before 'drain' event is emitted or when a write happens while another
          // objects is processed.
          // Objects can end up in a readable's buffer if there are no downstream processors ready to accept
          // new objects (either there are none or at least one of them is paused).
          //
          // Our data pipeline looks as follows:
          // bigtable stream -> ByteBuffer transform -> Reader transform -> ...
          //
          // But if we include buffers in this diagram this becomes more complicated:
          //
          // (bigtable stream -> [readable buffer])
          // -> ([writable buffer] -> ByteBuffer transform() -> [readable buffer])
          // -> ([writable buffer] -> Reader transform() -> [readable buffer])
          // -> ...
          //
          // and each of these buffers can buffer requests that were (in readable) or were not (in readable)
          // already passed to _transform() method.
          // During the retry we have to recreate bigtable stream and discard all data stored in
          // the ByteBuffer, and perform a new request with an appropriate resumeToken.
          //
          // Passing an appropriate resumeToken is crucial to prevent duplicate or lost responses.
          //
          // So, how to obtain a resumption token? Let's try a few options:
          // We cannot take last resumeToken that was seen by ByteBuffer's _transform() method
          // - it is possible that there are some unprocessed events in ByteBuffer's writable buffer
          // that will be processed at some point.
          // The same applies to Reader's _transform(), writeable buffers are still there.
          //
          // Thus we see that we have to consider events waiting in the buffers and wait until they are
          // processed.
          //
          // We cannot just wait until all events are processed by byteBuffer's _transform()
          // - there still might be some events left in byteBuffer's readable buffer that we don't want
          // to discard.
          //
          // Our solution here is to wait until all events that are present in Reader's writable buffer
          // will be processed and use last resumeToken seen by the Reader to make a new request.
          //
          // We will detach (unpipe) the ByteBuffer from the Reader and wait until all requests that
          // were written to the Reader by the ByteBuffer were processed using _transform() method.
          // Thus we can be certain that all events written before detachment were processed by _transform()
          // method and last resumption token seen by the Reader is the correct one to use.
          //
          // Details of how processing all requests is detected are in onWritableDrain function.
          //
          // So we will wait for clearing the buffer before making a new request and use last resumeToken
          // seen by the Reader to determine resumeToken to use in retry request.
          // This ensures that no responses will be lost - because everything seen by Reader's
          // _transform() was pushed towards the caller and won't be discarded,
          // and no duplicates will be encountered - because no more responses will be seen by Reader's
          // _transform() until new request is made.
          //
          // For simplicity we will drop previous bigtable stream and ByteBuffer transform and recreate them.
          // We could as well keep the ByteBuffer alive, but that would require us to cleanup it's internal
          // state and still perform all the waiting for reading the whole buffer, but just one step upstream.
          //
          // Please note that we cannot use gax's builtin streaming retries - we have no way of informing it
          // that we'd like to wait for an event to happen before the retry. An alternative approach would be
          // to purge all buffers of all streams just before the request is made, but there is no standard
          // API to do it. And it still wouldn't help us with gax, because we have no way of traversing all
          // the streams upstream of our ByteBuffer to purge their buffers or we'd have to rely on
          // implementation details.
          onWritableDrain(callerStream, abortController).then(
            (haveDrained: boolean) => {
              if (haveDrained) {
                retryTimer = setTimeout(makeNewRequest, nextRetryDelay);
              }
            }
          );
        } else {
          callerStream.emit('error', error);
        }
      })
      .on('data', () => {
        // Reset error count after a successful read so the backoff
        // time won't keep increasing when as stream had multiple errors
        numConsecutiveErrors = 0;
      })
      .on('end', () => {
        activeRequestStream = null;
      });
    rowStreamPipe(valueStream, callerStream);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    callerStream.end = (chunk?: any, encoding?: any, cb?: () => void) => {
      rowStreamUnpipe(valueStream, callerStream);
      // the caller has cancelled, abort all pending async operations.
      abortController.abort();
      return originalEnd(chunk, encoding, cb);
    };
  };
  makeNewRequest();
}
