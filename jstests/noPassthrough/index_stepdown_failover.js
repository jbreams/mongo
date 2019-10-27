/**
 * Confirms that index builds on a stepped down primary are not aborted and will
 * wait for a commitIndexBuild from the new primary before committing.
 * @tags: [
 *     requires_replication,
 * ]
 */
(function() {
"use strict";

load('jstests/libs/check_log.js');
load('jstests/noPassthrough/libs/index_build.js');

const rst = new ReplSetTest({
    // We want at least two electable nodes.
    nodes: [{}, {}, {arbiter: true}],
});
const nodes = rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const testDB = primary.getDB('test');
const coll = testDB.getCollection('test');

const enableTwoPhaseIndexBuild =
    assert.commandWorked(primary.adminCommand({getParameter: 1, enableTwoPhaseIndexBuild: 1}))
        .enableTwoPhaseIndexBuild;
if (!enableTwoPhaseIndexBuild) {
    jsTestLog('Two phase index builds not enabled, skipping test.');
    rst.stopSet();
    return;
}

assert.commandWorked(coll.insert({a: 1}));

// Start index build on primary, but prevent it from finishing.
IndexBuildTest.pauseIndexBuilds(primary);
const createIdx = IndexBuildTest.startIndexBuild(primary, coll.getFullName(), {a: 1});

// Wait for the index build to start on the secondary.
const secondary = rst.getSecondary();
const secondaryDB = secondary.getDB(testDB.getName());
const secondaryColl = secondaryDB.getCollection(coll.getName());
IndexBuildTest.waitForIndexBuildToStart(secondaryDB);
IndexBuildTest.assertIndexes(secondaryColl, 2, ["_id_"], ["a_1"], {includeBuildUUIDs: true});

const newPrimary = rst.getSecondary();
const newPrimaryDB = secondaryDB;
const newPrimaryColl = secondaryColl;

// Step down the primary.
// Expect failed createIndex command invocation in parallel shell due to stepdown.
// Before SERVER-44186, the index build will be aborted during stepdown.
assert.commandWorked(primary.adminCommand({replSetStepDown: 60, force: true}));
const exitCode = createIdx({checkExitSuccess: false});
assert.neq(0, exitCode, 'expected shell to exit abnormally due to index build being terminated');
checkLog.contains(primary, 'Index build interrupted: ');

// Unblock the index build on the old primary during the collection scanning phase.
IndexBuildTest.resumeIndexBuilds(primary);

// Step up the new primary.
rst.stepUp(newPrimary);

// A new index should not be present on the old primary because the index build was aborted.
IndexBuildTest.waitForIndexBuildToStop(testDB);
IndexBuildTest.assertIndexes(coll, 1, ['_id_']);

TestData.skipCheckDBHashes = true;
rst.stopSet();
})();