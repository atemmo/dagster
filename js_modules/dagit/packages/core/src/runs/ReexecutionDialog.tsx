import {useMutation} from '@apollo/client';
// eslint-disable-next-line no-restricted-imports
import {ProgressBar} from '@blueprintjs/core';
import {Button, Colors, DialogBody, DialogFooter, Dialog, Group, Icon, Mono} from '@dagster-io/ui';
import * as React from 'react';
import * as yaml from 'yaml';

import {useRepositoriesForRuns} from '../workspace/useRepositoryForRun';

import {NavigationBlock} from './NavitationBlock';
import {getReexecutionVariables, LAUNCH_PIPELINE_REEXECUTION_MUTATION} from './RunUtils';
import {
  LaunchPipelineReexecution,
  LaunchPipelineReexecution_launchPipelineReexecution_InvalidStepError,
  LaunchPipelineReexecution_launchPipelineReexecution_PipelineNotFoundError,
  LaunchPipelineReexecution_launchPipelineReexecution_PythonError,
  LaunchPipelineReexecution_launchPipelineReexecution_RunConfigValidationInvalid,
} from './types/LaunchPipelineReexecution';
import {RunTableRunFragment} from './types/RunTableRunFragment';

export interface Props {
  isOpen: boolean;
  onClose: () => void;
  onComplete: (reexecutionState: ReexecutionState) => void;
  selectedRuns: {[id: string]: RunTableRunFragment};
}

type Error =
  | LaunchPipelineReexecution_launchPipelineReexecution_InvalidStepError
  | LaunchPipelineReexecution_launchPipelineReexecution_PipelineNotFoundError
  | LaunchPipelineReexecution_launchPipelineReexecution_RunConfigValidationInvalid
  | LaunchPipelineReexecution_launchPipelineReexecution_PythonError
  | undefined;

const errorText = (error: Error) => {
  if (!error) {
    return 'Unknown error';
  }
  switch (error.__typename) {
    case 'ConflictingExecutionParamsError':
      return 'Conflicting execution parameters';
    case 'InvalidOutputError':
      return 'Invalid output';
    case 'InvalidStepError':
      return 'Invalid step';
    case 'NoModeProvidedError':
      return 'No mode provided';
    case 'PipelineNotFoundError':
      return 'Job not found in workspace';
    case 'PresetNotFoundError':
      return 'Preset not found';
    case 'PythonError':
      return error.message;
    case 'RunConfigValidationInvalid':
      return 'Run config invalid';
    case 'RunConflict':
      return 'Run conflict';
    case 'UnauthorizedError':
      return 'Re-execution not authorized';
    default:
      return 'Unknown error';
  }
};

export type ReexecutionState = {completed: number; errors: {[id: string]: Error}};

type ReexecutionDialogState = {
  frozenRuns: SelectedRuns;
  step: 'initial' | 'reexecuting' | 'completed';
  reexecution: ReexecutionState;
};

type SelectedRuns = {[id: string]: RunTableRunFragment};

const initializeState = (selectedRuns: SelectedRuns): ReexecutionDialogState => {
  return {
    frozenRuns: selectedRuns,
    step: 'initial',
    reexecution: {completed: 0, errors: {}},
  };
};

type ReexecutionDialogAction =
  | {type: 'reset'; frozenRuns: SelectedRuns}
  | {type: 'start'}
  | {type: 'reexecution-success'}
  | {type: 'reexecution-error'; id: string; error: Error}
  | {type: 'complete'};

const reexecutionDialogReducer = (
  prevState: ReexecutionDialogState,
  action: ReexecutionDialogAction,
): ReexecutionDialogState => {
  switch (action.type) {
    case 'reset':
      return initializeState(action.frozenRuns);
    case 'start':
      return {...prevState, step: 'reexecuting'};
    case 'reexecution-success': {
      const {reexecution} = prevState;
      return {
        ...prevState,
        step: 'reexecuting',
        reexecution: {...reexecution, completed: reexecution.completed + 1},
      };
    }
    case 'reexecution-error': {
      const {reexecution} = prevState;
      return {
        ...prevState,
        step: 'reexecuting',
        reexecution: {
          ...reexecution,
          completed: reexecution.completed + 1,
          errors: {...reexecution.errors, [action.id]: action.error},
        },
      };
    }
    case 'complete':
      return {...prevState, step: 'completed'};
  }
};

export const ReexecutionDialog = (props: Props) => {
  const {isOpen, onClose, onComplete, selectedRuns} = props;

  // Freeze the selected IDs, since the list may change as runs continue processing and
  // re-executing. We want to preserve the list we're given.
  const frozenRuns = React.useRef<SelectedRuns>(selectedRuns);

  const [state, dispatch] = React.useReducer(
    reexecutionDialogReducer,
    frozenRuns.current,
    initializeState,
  );

  const repositoryMatches = useRepositoriesForRuns(state.frozenRuns);

  const count = Object.keys(state.frozenRuns).length;

  // If the dialog is newly open, update state to match the frozen list.
  React.useEffect(() => {
    if (isOpen) {
      dispatch({type: 'reset', frozenRuns: frozenRuns.current});
    }
  }, [isOpen]);

  // If the dialog is not open, update the ref so that the frozen list will be entered
  // into state the next time the dialog opens.
  React.useEffect(() => {
    if (!isOpen) {
      frozenRuns.current = selectedRuns;
    }
  }, [isOpen, selectedRuns]);

  const [reexecute] = useMutation<LaunchPipelineReexecution>(LAUNCH_PIPELINE_REEXECUTION_MUTATION);

  const mutate = async () => {
    dispatch({type: 'start'});

    const runList = Object.keys(state.frozenRuns);
    for (let ii = 0; ii < runList.length; ii++) {
      const runId = runList[ii];
      const run = state.frozenRuns[runId];
      const repositoryMatch = repositoryMatches[runId];
      if (!repositoryMatch) {
        dispatch({
          type: 'reexecution-error',
          id: runId,
          error: {
            __typename: 'PipelineNotFoundError',
            message: 'Job not found in workspace',
          },
        });
      } else {
        const runConfig = yaml.parse(run.runConfigYaml);
        const {data} = await reexecute({
          variables: getReexecutionVariables({
            run: {...run, runConfig},
            style: {type: 'all'},
            repositoryLocationName: repositoryMatch.match.repositoryLocation.name,
            repositoryName: repositoryMatch.match.repository.name,
          }),
        });

        if (data?.launchPipelineReexecution.__typename === 'LaunchRunSuccess') {
          dispatch({type: 'reexecution-success'});
        } else {
          dispatch({type: 'reexecution-error', id: runId, error: data?.launchPipelineReexecution});
        }
      }
    }

    dispatch({type: 'complete'});
    onComplete(state.reexecution);
  };

  // const showCheckbox = Object.keys(state.frozenRuns).some((id) => state.frozenRuns[id]);

  const progressContent = () => {
    switch (state.step) {
      case 'initial':
        if (!count) {
          return (
            <Group direction="column" spacing={16}>
              <div>No runs selected for re-execution.</div>
              <div>The runs you selected may already have finished executing.</div>
            </Group>
          );
        }

        return (
          <Group direction="column" spacing={16}>
            <div>
              {`${count} ${
                count === 1 ? 'run' : 'runs'
              } will be re-executed. Do you wish to continue?`}
            </div>
          </Group>
        );
      case 'reexecuting':
      case 'completed':
        const value = count > 0 ? state.reexecution.completed / count : 1;
        return (
          <Group direction="column" spacing={8}>
            <ProgressBar intent="primary" value={Math.max(0.1, value)} animate={value < 1} />
            {state.step === 'reexecuting' ? (
              <NavigationBlock message="Re-execution in progress, please do not navigate away yet." />
            ) : null}
          </Group>
        );
      default:
        return null;
    }
  };

  const buttons = () => {
    switch (state.step) {
      case 'initial':
        if (!count) {
          return (
            <Button intent="none" onClick={onClose}>
              OK
            </Button>
          );
        }

        return (
          <>
            <Button intent="none" onClick={onClose}>
              Cancel
            </Button>
            <Button intent="primary" onClick={mutate}>
              {`Re-execute ${`${count} ${count === 1 ? 'run' : 'runs'}`}`}
            </Button>
          </>
        );
      case 'reexecuting':
        return (
          <Button intent="primary" disabled>
            {`Re-executing ${`${count} ${count === 1 ? 'run' : 'runs'}...`}`}
          </Button>
        );
      case 'completed':
        return (
          <Button intent="primary" onClick={onClose}>
            Done
          </Button>
        );
    }
  };

  const completionContent = () => {
    if (state.step === 'initial') {
      return null;
    }

    if (state.step === 'reexecuting') {
      return <div>Please do not close the window or navigate away during re-execution.</div>;
    }

    const errors = state.reexecution.errors;
    const errorCount = Object.keys(errors).length;
    const successCount = state.reexecution.completed - errorCount;

    return (
      <Group direction="column" spacing={8}>
        {successCount ? (
          <Group direction="row" spacing={8} alignItems="flex-start">
            <Icon name="check_circle" color={Colors.Green500} />
            <div>
              {`Successfully requested re-execution for ${successCount} ${
                successCount === 1 ? 'run' : `runs`
              }.`}
            </div>
          </Group>
        ) : null}
        {errorCount ? (
          <Group direction="column" spacing={8}>
            <Group direction="row" spacing={8} alignItems="flex-start">
              <Icon name="warning" color={Colors.Yellow500} />
              <div>
                {`Could not request re-execution for ${errorCount} ${
                  errorCount === 1 ? 'run' : 'runs'
                }:`}
              </div>
            </Group>
            <ul>
              {Object.keys(errors).map((runId) => (
                <li key={runId}>
                  <Group direction="row" spacing={8} alignItems="baseline">
                    <Mono>{runId.slice(0, 8)}</Mono>
                    {errors[runId] ? <div>{errorText(errors[runId])}</div> : null}
                  </Group>
                </li>
              ))}
            </ul>
          </Group>
        ) : null}
      </Group>
    );
  };

  const canQuicklyClose = state.step !== 'reexecuting';

  return (
    <Dialog
      isOpen={isOpen}
      title="Re-execute runs"
      canEscapeKeyClose={canQuicklyClose}
      canOutsideClickClose={canQuicklyClose}
      onClose={onClose}
    >
      <DialogBody>
        <Group direction="column" spacing={24}>
          {progressContent()}
          {completionContent()}
        </Group>
      </DialogBody>
      <DialogFooter>{buttons()}</DialogFooter>
    </Dialog>
  );
};
