// A replicated publish is a message, not a record, so no stored version ties a re-delivery to it. A resume
// re-streams the origin's log from below what this node applied, and every re-applied publish is a new log
// entry that live subscribers receive again.
require('../testUtils');
const assert = require('node:assert');
const { setupTestDBPath } = require('../testUtils');
const { waitFor } = require('../waitFor.js');
const { table } = require('#src/resources/databases');
const { setMainIsWorker } = require('#src/server/threads/manageThreads');
const { transaction } = require('#src/resources/transaction');
const { getIdOfRemoteNode } = require('#src/resources/nodeIdMapping');

// LMDB keys its log by local time, so an applied publish carries no origin log key to find it by.
const isLMDB = process.env.HARPER_STORAGE_ENGINE === 'lmdb';

describe('Re-delivered replicated publishes', function () {
	this.timeout(10000);
	let Topics, peer, otherPeer, subscription;
	let received = [];
	let lastLogKey = Date.now() - 3_600_000;
	const originClock = () => (lastLogKey += 10_000);
	let markers = 0;

	before(async function () {
		if (isLMDB) return this.skip();
		setupTestDBPath();
		setMainIsWorker(true);
		Topics = table({
			table: 'RedeliveredPublishes',
			database: 'redeliveredPublishes',
			attributes: [{ name: 'id', isPrimaryKey: true }, { name: 'n' }],
			audit: true,
		});
		peer = getIdOfRemoteNode('publish-peer-a', Topics.auditStore);
		otherPeer = getIdOfRemoteNode('publish-peer-b', Topics.auditStore);
		subscription = await Topics.subscribe({ isCollection: true, omitCurrent: true });
		subscription.on('data', (event) => {
			if (event.type === 'message') received.push(`${event.id}:${event.value?.n}`);
		});
	});
	after(() => subscription?.end());
	beforeEach(() => {
		received = [];
	});

	function applyFrame(logKey, publishes, nodeId = peer) {
		const context = { source: {}, sourceApply: true, timestamp: logKey };
		return transaction(context, async () => {
			for (const { id, n, version = logKey } of publishes) {
				const resource = await Topics.getResource(id, context);
				resource._writePublish(id, { n }, { isNotification: true, nodeId, version });
			}
		});
	}

	// Entries in one origin's log are delivered in log order, so once a later publish from that origin
	// arrives, anything an earlier apply logged has been delivered too.
	async function drain(nodeId = peer) {
		const marker = `marker-${++markers}`;
		await applyFrame(originClock(), [{ id: marker, n: 0 }], nodeId);
		await waitFor(() => received.includes(`${marker}:0`), { timeout: 5000, message: `${marker} not delivered` });
		received = received.filter((delivery) => !delivery.startsWith('marker-'));
	}

	function loggedMessages(id) {
		return [...Topics.auditStore.getRange({ start: 1, log: peer })].filter(
			(entry) => entry.recordId === id && entry.type === 'message'
		).length;
	}

	it('delivers a publish re-delivered at its origin key once', async () => {
		const logKey = originClock();
		await applyFrame(logKey, [{ id: 'sensor/1', n: 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor/1:1']);
		for (let i = 0; i < 3; i++) await applyFrame(logKey, [{ id: 'sensor/1', n: 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor/1:1']);
		assert.equal(loggedMessages('sensor/1'), 1);
	});

	it('delivers a re-streamed run once, after newer publishes already arrived', async () => {
		const keys = [originClock(), originClock(), originClock()];
		for (const [i, logKey] of keys.entries()) await applyFrame(logKey, [{ id: 'sensor/run', n: i }]);
		const newer = originClock();
		await applyFrame(newer, [{ id: 'sensor/run', n: 3 }]);
		// a resume from below the first key re-streams all four
		for (const [i, logKey] of [...keys, newer].entries()) await applyFrame(logKey, [{ id: 'sensor/run', n: i }]);
		await drain();
		assert.deepEqual(received, ['sensor/run:0', 'sensor/run:1', 'sensor/run:2', 'sensor/run:3']);
	});

	it('delivers every publish of a re-delivered transaction once, including two to one topic', async () => {
		const logKey = originClock();
		const frame = [
			{ id: 'sensor/2', n: 1 },
			{ id: 'sensor/2', n: 2 },
			{ id: 'sensor/3', n: 1 },
		];
		await applyFrame(logKey, frame);
		await applyFrame(logKey, frame);
		await drain();
		assert.deepEqual(received.sort(), ['sensor/2:1', 'sensor/2:2', 'sensor/3:1']);
	});

	it('delivers a publish to a multi-segment id re-delivered at its origin key once', async () => {
		const logKey = originClock();
		await applyFrame(logKey, [{ id: ['sensor', 'a', 'b'], n: 1 }]);
		await applyFrame(logKey, [{ id: ['sensor', 'a', 'b'], n: 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor,a,b:1']);
	});

	it('delivers a publish once when its transaction also wrote the record it lands on', async () => {
		const logKey = originClock();
		const frame = () => {
			const context = { source: {}, sourceApply: true, timestamp: logKey };
			return transaction(context, async () => {
				const resource = await Topics.getResource('sensor/8', context);
				await resource._writeUpdate('sensor/8', { id: 'sensor/8', n: 'state' }, true, {
					isNotification: true,
					nodeId: peer,
					version: logKey,
				});
				resource._writePublish('sensor/8', { n: 1 }, { isNotification: true, nodeId: peer, version: logKey });
			});
		};
		await frame();
		await frame();
		await drain();
		assert.deepEqual(received, ['sensor/8:1']);
		assert.equal((await Topics.get('sensor/8')).n, 'state');
	});

	it('delivers a late publish that is new to this node, below keys it already applied', async () => {
		const late = originClock();
		await applyFrame(originClock(), [{ id: 'sensor/4', n: 2 }]);
		await applyFrame(late, [{ id: 'sensor/4', n: 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor/4:2', 'sensor/4:1']);
	});

	it('delivers a newer publish once when a late publish to its topic applied after it', async () => {
		const late = originClock();
		const newer = originClock();
		await applyFrame(newer, [{ id: 'sensor/9', n: 2 }]);
		await applyFrame(late, [{ id: 'sensor/9', n: 1 }]);
		// a resume from below both re-sends them in the origin's commit order
		await applyFrame(newer, [{ id: 'sensor/9', n: 2 }]);
		await applyFrame(late, [{ id: 'sensor/9', n: 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor/9:2', 'sensor/9:1']);
	});

	it('delivers a publish whose version differs from its log key once', async () => {
		const logKey = originClock();
		await applyFrame(logKey, [{ id: 'sensor/10', n: 1, version: logKey - 5_000 }]);
		await applyFrame(logKey, [{ id: 'sensor/10', n: 1, version: logKey - 5_000 }]);
		await drain();
		assert.deepEqual(received, ['sensor/10:1']);
	});

	it('delivers each publish of a wide re-delivered transaction once, reading the log once per delivery', async () => {
		const logKey = originClock();
		const frame = Array.from({ length: 2000 }, (_, n) => ({ id: `wide/${n}`, n }));
		await applyFrame(logKey, frame);
		const originalRange = Topics.auditStore.getRange;
		let reads = 0;
		Topics.auditStore.getRange = function (options, ...args) {
			if (options?.start === logKey) reads++;
			return originalRange.call(this, options, ...args);
		};
		try {
			await applyFrame(logKey, frame);
		} finally {
			Topics.auditStore.getRange = originalRange;
		}
		await drain();
		assert.equal(received.length, 2000);
		assert.equal(new Set(received).size, 2000);
		assert.equal(reads, 1);
	});

	it('delivers publishes from two origins at the same key and topic', async () => {
		const logKey = originClock();
		await applyFrame(logKey, [{ id: 'sensor/5', n: 1 }]);
		await applyFrame(logKey, [{ id: 'sensor/5', n: 2 }], otherPeer);
		await drain();
		await drain(otherPeer);
		assert.deepEqual(received.sort(), ['sensor/5:1', 'sensor/5:2']);
	});

	it('delivers a publish at a known key under a different version', async () => {
		const logKey = originClock();
		await applyFrame(logKey, [{ id: 'sensor/6', n: 1, version: logKey }]);
		await applyFrame(logKey, [{ id: 'sensor/6', n: 2, version: logKey - 1 }]);
		await drain();
		assert.deepEqual(received, ['sensor/6:1', 'sensor/6:2']);
	});

	it('leaves the record a keyed publish lands on unchanged when it is re-delivered', async () => {
		const putKey = originClock();
		const context = { source: {}, sourceApply: true, timestamp: putKey };
		await transaction(context, async () => {
			const resource = await Topics.getResource('sensor/7', context);
			await resource._writeUpdate('sensor/7', { id: 'sensor/7', n: 'retained' }, true, {
				isNotification: true,
				nodeId: peer,
				version: putKey,
			});
		});
		const logKey = originClock();
		await applyFrame(logKey, [{ id: 'sensor/7', n: 1 }]);
		await applyFrame(logKey, [{ id: 'sensor/7', n: 1 }]);
		await drain();
		assert.deepEqual(
			received.filter((delivery) => delivery !== 'sensor/7:retained'),
			['sensor/7:1']
		);
		assert.equal((await Topics.get('sensor/7')).n, 'retained');
	});

	it('still delivers every local publish', async () => {
		await Topics.publish('local/1', { n: 1 });
		await Topics.publish('local/1', { n: 1 });
		await waitFor(() => received.length === 2, { timeout: 5000, message: `received ${received}` });
		assert.deepEqual(received, ['local/1:1', 'local/1:1']);
	});
});
