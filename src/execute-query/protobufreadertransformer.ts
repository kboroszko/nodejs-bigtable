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

import {Transform, TransformOptions, TransformCallback} from 'stream';
// eslint-disable-next-line @typescript-eslint/no-var-requires
import {google} from '../../protos/protos';
import {MetadataConsumer} from './metadataconsumer';

class DrainGuard {
  callback: () => void;

  constructor(callback: () => void) {
    this.callback = callback;
  }
}

type BatchAndToken = [Array<Uint8Array>, Uint8Array];

/**
 * This transformer is responsible for deserializing bytes sent from the
 * server to an appropriate object which can be used to construct rows.
 * Right now only google.bigtable.v2.ProtoRows is supported.
 */
export class ProtobufReaderTransformer extends Transform {
  valuesBuffer: google.bigtable.v2.IValue[];
  metadataConsumer: MetadataConsumer;
  resumeToken: Uint8Array | null;

  constructor(metadataConsumer: MetadataConsumer, opts?: TransformOptions) {
    super({...opts, objectMode: true, highWaterMark: 1024});
    this.metadataConsumer = metadataConsumer;
    this.valuesBuffer = [];
    this.resumeToken = null;
  }

  _transform(
    batchAndToken: BatchAndToken | DrainGuard,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ) {
    if (batchAndToken instanceof DrainGuard) {
      batchAndToken.callback();
    } else {
      const maybeMetadata = this.metadataConsumer.getMetadata();
      if (maybeMetadata) {
        const [batch, resumeToken] = batchAndToken;
        this.resumeToken = resumeToken;

        if (batch.length > 0) {
          const rows = google.bigtable.v2.ProtoRows.decode(
            Buffer.concat(batch)
          );
          for (const value of rows.values) {
            this.valuesBuffer.push(value);
          }

          const expectedLength = maybeMetadata.columns.length;

          if (this.valuesBuffer.length >= expectedLength) {
            let i = 0;
            for (; i < this.valuesBuffer.length; i += expectedLength) {
              if (i + expectedLength > this.valuesBuffer.length) {
                callback(
                  new Error('Internal error - received incomplete row.')
                );
                return;
              }

              this.push(this.valuesBuffer.slice(i, i + expectedLength));
            }
            this.valuesBuffer = [];
          } else {
            callback(new Error('Internal error - received incomplete row.'));
            return;
          }
        }
      } else {
        return callback(
          new Error('Internal error - missing metadata response')
        );
      }
    }

    callback();
  }

  static createDrainGuard(callback: () => void) {
    return new DrainGuard(callback);
  }
}
