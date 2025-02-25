// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {Transform, TransformCallback} from 'stream';
// eslint-disable-next-line @typescript-eslint/no-var-requires
import {google} from '../../protos/protos';

import {MetadataConsumer} from './metadataconsumer';
import {ensureUint8Array} from './values';

/**
 * stream.Transform which buffers bytes from `ExecuteQuery` responses until
 * resumeToken is received. At that point all buffered messages are passed
 * forward.
 */
export class ByteBufferTransformer extends Transform {
  messageBuffer: Uint8Array[] = [];
  metadataConsumer: MetadataConsumer;
  protoBytesEncoding?: BufferEncoding;

  constructor(
    metadataConsumer: MetadataConsumer,
    protoBytesEncoding?: BufferEncoding
  ) {
    super({objectMode: true, highWaterMark: 0});
    this.metadataConsumer = metadataConsumer;
    this.protoBytesEncoding = protoBytesEncoding;
  }

  _transform(
    chunk: google.bigtable.v2.ExecuteQueryResponse,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ) {
    let error: Error | null = null;
    const reponse = chunk as google.bigtable.v2.ExecuteQueryResponse;
    switch (reponse.response) {
      case 'metadata':
        try {
          this.metadataConsumer.consume(reponse.metadata!);
        } catch (e) {
          error = e as Error;
        }
        break;

      case 'results': {
        let handled = false;
        if (reponse.results?.protoRowsBatch?.batchData?.length) {
          this.messageBuffer.push(
            ensureUint8Array(
              reponse.results.protoRowsBatch.batchData,
              this.protoBytesEncoding
            )
          );
          handled = true;
        }
        if (reponse.results!.resumeToken) {
          const resumeToken = ensureUint8Array(
            reponse.results!.resumeToken,
            this.protoBytesEncoding
          );
          this.push([this.messageBuffer, resumeToken]);
          this.messageBuffer = [];
          handled = true;
        }
        if (!handled) {
          error = Error(
            'Internal Error. Response did not contain any results!'
          );
        }
        break;
      }
      default:
        error = Error(
          `Internal Error. Response contains unknown type ${reponse.response}`
        );
    }
    callback(error);
  }

  _flush(callback: TransformCallback): void {
    if (this.messageBuffer.length > 0) {
      return callback(
        new Error('Internal Error. Last message did not contain a resumeToken.')
      );
    }
    callback();
  }
}
