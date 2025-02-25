// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
import * as promisify from '@google-cloud/promisify';
import * as assert from 'assert';
import {before, beforeEach, afterEach, describe, it} from 'mocha';
import * as sinon from 'sinon';
import * as proxyquire from 'proxyquire';
import {grpc} from 'google-gax';
import * as inst from '../src/instance';
import {Bigtable} from '../src';
import * as pumpify from 'pumpify';
import {
  ArrayReadableStream,
  createMetadata,
  createProtoRows,
  pbType,
} from './utils/proto-bytes';
import {QueryResultRow} from '../src/execute-query/values';

const sandbox = sinon.createSandbox();

const fakePromisify = Object.assign({}, promisify, {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  promisifyAll(klass: Function, options: any) {
    if (klass.name !== 'Instance') {
      return;
    }
    assert.deepStrictEqual(options.exclude, [
      'appProfile',
      'cluster',
      'table',
      'getBackupsStream',
      'getTablesStream',
      'getAppProfilesStream',
    ]);
  },
});

describe('Bigtable/ExecuteQueryE2E', () => {
  const INSTANCE_ID = 'my-instance';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const BIGTABLE = {
    projectName: 'projects/my-project',
    projectId: 'my-project',
    request: () => {},
  } as Bigtable;
  let Instance: typeof inst.Instance;
  let instance: inst.Instance;

  let clock: sinon.SinonFakeTimers;

  before(() => {
    Instance = proxyquire('../src/instance.js', {
      '@google-cloud/promisify': fakePromisify,
      pumpify,
    }).Instance;

    clock = sinon.useFakeTimers({
      toFake: ['setTimeout', 'clearTimeout'],
    });
  });

  beforeEach(() => {
    instance = new Instance(BIGTABLE, INSTANCE_ID);
  });

  afterEach(() => {
    clock.restore();
    sandbox.restore();
  });

  describe('happy_path', () => {
    it('empty response', done => {
      BIGTABLE.request = () => new ArrayReadableStream([]) as any;
      instance.executeQuery('query string', (err, rows) => {
        assert.equal(err instanceof Error, true);
        assert.ifError(rows);
        done();
      });
    });

    it('only metadata', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
        ]) as any;
      instance.executeQuery('query string', (err, rows) => {
        assert.deepEqual(rows, []);
        done();
      });
    });

    it('metadata & token', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1'),
        ]) as any;
      instance.executeQuery('query string', (err, rows) => {
        assert.deepEqual(rows, []);
        done();
      });
    });

    it('3 rows', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1', {intValue: 1}),
          createProtoRows('token2', {intValue: 2}),
          createProtoRows('token3', {intValue: 3}),
        ]) as any;
      instance.executeQuery('query string', (err, rows) => {
        assert.strictEqual(rows?.length, 3);
        done();
      });
    });

    it('3 rows but ended', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1', {intValue: 1}),
          createProtoRows('token2', {intValue: 2}),
          {
            callback: () => {
              clock.runAll();
            },
          },
          createProtoRows('token3', {intValue: 3}),
        ]) as any;
      const recieved_data: QueryResultRow[] = [];
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        recieved_data.push(v);
        if (recieved_data.length === 2) {
          stream.end();
        }
      });
      stream.on('end', () => {
        assert.strictEqual(recieved_data.length, 2);
        done();
      });

      clock.runAll();
    });

    it('ended after all data retrieved', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1', {intValue: 1}),
          createProtoRows('token2', {intValue: 2}),
          {
            callback: () => {
              clock.runAll();
            },
          },
          createProtoRows('token3', {intValue: 3}),
        ]) as any;
      const recieved_data: QueryResultRow[] = [];
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        recieved_data.push(v);
      });
      stream.on('end', () => {
        assert.strictEqual(recieved_data.length, 3);
        setTimeout(() => {
          stream.end();
          setTimeout(done, 100);
        }, 0);
      });

      clock.runAll();
    });
  });

  describe('non retryable errors', () => {
    it('immediate error', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          {status: grpc.status.INTERNAL, message: 'fail', code: 500},
        ]) as any;
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('error', (e: any) => {
        assert.strictEqual(e.code, 500);
        assert.strictEqual(e.message, 'fail');
        assert.strictEqual(e.status, 13);
        done();
      });
    });

    it('error after metadata', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          {status: grpc.status.INTERNAL, message: 'fail', code: 500},
        ]) as any;
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('error', (e: any) => {
        assert.strictEqual(e.code, 500);
        assert.strictEqual(e.message, 'fail');
        assert.strictEqual(e.status, 13);
        done();
      });
    });

    it('error after some rows', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1', {intValue: 1}),
          createProtoRows('token2', {intValue: 2}),
          {status: grpc.status.INTERNAL, message: 'fail', code: 500},
        ]) as any;
      const recieved_data: QueryResultRow[] = [];
      let errorOccured = false;
      const doneAfterAllEvents = () => {
        // the error can be forwarded by the pumpify faster than the data events
        // we are done after all events have been delivered regardless of the order.
        if (recieved_data.length === 2 && errorOccured) {
          done();
        }
      };
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        recieved_data.push(v);
        doneAfterAllEvents();
      });
      stream.on('error', (e: any) => {
        // failed on expected error
        assert.strictEqual(e.code, 500);
        assert.strictEqual(e.message, 'fail');
        assert.strictEqual(e.status, 13);
        errorOccured = true;
        doneAfterAllEvents();
      });
    });

    it('error because stream ends without resumeToken', done => {
      BIGTABLE.request = () =>
        new ArrayReadableStream([
          createMetadata(['f1', pbType({int64Type: {}})]),
          createProtoRows('token1', {intValue: 1}),
          createProtoRows(undefined, {intValue: 2}),
          {status: grpc.status.INTERNAL, message: 'fail', code: 500},
        ]) as any;
      const recieved_data: QueryResultRow[] = [];
      let errorOccured = false;
      const doneAfterAllEvents = () => {
        // the error can be forwarded by the pumpify faster than the data events
        // we are done after all events have been delivered regardless of the order.
        if (recieved_data.length === 1 && errorOccured) {
          done();
        }
      };
      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        recieved_data.push(v);
        doneAfterAllEvents();
      });
      stream.on('error', (e: any) => {
        // failed on expected error
        assert.strictEqual(e.code, 500);
        assert.strictEqual(e.message, 'fail');
        assert.strictEqual(e.status, 13);
        errorOccured = true;
        doneAfterAllEvents();
      });
    });
  });

  describe('retryable errors', () => {
    it('error after metadata', done => {
      let requestCount = 0;
      BIGTABLE.request = ({gaxOpts}) => {
        requestCount += 1;
        return [
          new ArrayReadableStream([
            createMetadata(['f1', pbType({int64Type: {}})]),
            {code: 4, status: '', message: ''},
          ]),
          new ArrayReadableStream([
            createMetadata(['f1', pbType({int64Type: {}})]),
            createProtoRows('token1', {intValue: 1}),
          ]),
        ][requestCount - 1] as any;
      };

      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        assert.strictEqual(v.values[0], BigInt(1));
        done();
      });

      stream.on('error', e => {
        throw e;
      });
      clock.runAll();
    });

    it('error after metadata and some data', done => {
      let requestCount = 0;
      BIGTABLE.request = ({gaxOpts}) => {
        requestCount += 1;
        return [
          new ArrayReadableStream([
            createMetadata(['f1', pbType({int64Type: {}})]),
            createProtoRows(undefined, {intValue: 1}),
            {code: 4, status: '', message: ''},
          ]),
          new ArrayReadableStream([
            createMetadata(['f1', pbType({int64Type: {}})]),
            createProtoRows('token1', {intValue: 2}),
          ]),
        ][requestCount - 1] as any;
      };

      const stream = instance.createExecuteQueryStream({query: 'query string'});
      stream.on('data', v => {
        assert.strictEqual(v.values[0], BigInt(2));
        done();
      });
      stream.on('error', e => {
        throw e;
      });
      clock.runAll();
    });
  });
});
