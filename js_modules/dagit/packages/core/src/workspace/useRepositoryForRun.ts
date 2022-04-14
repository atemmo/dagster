import * as React from 'react';

import {RunFragmentForRepositoryMatch} from '../runs/types/RunFragmentForRepositoryMatch';

import {DagsterRepoOption, useRepositoryOptions} from './WorkspaceContext';
import {findRepoContainingPipeline, repoContainsPipeline} from './findRepoContainingPipeline';

type MatchType = {
  match: DagsterRepoOption;
  type: 'origin-and-snapshot' | 'origin-only' | 'snapshot-only' | 'pipeline-name-only';
};

const findRepositoryMatchForRun = (
  run: RunFragmentForRepositoryMatch | null | undefined,
  options: DagsterRepoOption[],
): MatchType | null => {
  const findRepoMatch = () => {
    if (!run) {
      return null;
    }

    const pipelineName = run.pipelineName;
    // Try to match the pipeline name within the specified origin, if possible.
    const origin = run.repositoryOrigin;

    if (!origin) {
      return null;
    }

    const location = origin?.repositoryLocationName;
    const name = origin?.repositoryName;

    const match = options.find(
      (option) => option.repository.name === name && option.repositoryLocation.name === location,
    );

    // The origin repo is loaded. Verify that a pipeline with this name exists and return the match if so.
    return match && repoContainsPipeline(match, pipelineName) ? match : null;
  };

  const findSnapshotMatches = () => {
    if (!run) {
      return null;
    }

    const pipelineName = run.pipelineName;
    const snapshotId = run.pipelineSnapshotId;

    // Find the repository that contains the specified pipeline name and snapshot ID, if any.
    if (pipelineName && snapshotId) {
      const snapshotMatches = findRepoContainingPipeline(options, pipelineName, snapshotId);
      if (snapshotMatches.length) {
        return snapshotMatches;
      }
    }

    return null;
  };

  const findPipelineNameMatches = () => {
    if (!run) {
      return null;
    }

    const pipelineName = run.pipelineName;

    // There is no origin repo. Find any repos that might contain a matching pipeline name.
    const possibleMatches = findRepoContainingPipeline(options, pipelineName);
    return possibleMatches.length ? possibleMatches : null;
  };

  const repoMatch = findRepoMatch();
  const snapshotMatches = findSnapshotMatches();
  const pipelineNameMatches = findPipelineNameMatches();

  if (repoMatch) {
    if (snapshotMatches) {
      const repoAndSnapshotMatch = snapshotMatches.find(
        (repoOption) =>
          repoOption.repository.name === repoMatch.repository.name &&
          repoOption.repositoryLocation.name === repoMatch.repositoryLocation.name,
      );
      if (repoAndSnapshotMatch) {
        return {match: repoAndSnapshotMatch, type: 'origin-and-snapshot'};
      }
    }

    return {match: repoMatch, type: 'origin-only'};
  }

  if (snapshotMatches) {
    return {match: snapshotMatches[0], type: 'snapshot-only'};
  }

  if (pipelineNameMatches) {
    return {match: pipelineNameMatches[0], type: 'pipeline-name-only'};
  }

  return null;
};

/**
 * Given a Run fragment, find the repository that contains its pipeline.
 */
export const useRepositoryForRun = (
  run: RunFragmentForRepositoryMatch | null | undefined,
): MatchType | null => {
  const {options} = useRepositoryOptions();
  return React.useMemo(() => findRepositoryMatchForRun(run, options), [run, options]);
};

export const useRepositoriesForRuns = (runs: {
  [runId: string]: RunFragmentForRepositoryMatch | null | undefined;
}): {[runId: string]: MatchType | null} => {
  const {options} = useRepositoryOptions();
  return React.useMemo(() => {
    const matches: {[runId: string]: MatchType | null} = {};
    for (const runId in runs) {
      matches[runId] = findRepositoryMatchForRun(runs[runId], options);
    }
    return matches;
  }, [options, runs]);
};
