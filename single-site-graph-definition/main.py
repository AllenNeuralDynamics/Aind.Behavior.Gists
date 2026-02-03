import logging
from typing import Any, Literal
from aind_behavior_vr_foraging import task_logic
from aind_behavior_services.task_logic import distributions
import numpy as np
from aind_behavior_curriculum import Stage, TrainerState

logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)

# We will implement


def make_patch(
    label: str,
    state_index: int,
    odor_index: Literal[1, 2, 3],
    p_reward: float,
    reward_amount: float = 5.0,
    inter_patch_min_length: float = 30,
    inter_patch_mean_length: float = 60,
    inter_patch_max_length: float = 190,
    stop_duration: float = 1.5,
    inter_site_length: float = 15,
    reward_site_length: float = 50,
):
    return task_logic.Patch(
        label=label,
        state_index=state_index,
        odor_specification=task_logic.OdorSpecification(
            index=odor_index,
        ),
        patch_terminators=[
            task_logic.PatchTerminatorOnChoice(count=task_logic.scalar_value(1)),
            task_logic.PatchTerminatorOnRejection(count=task_logic.scalar_value(1)),
        ],
        reward_specification=task_logic.RewardSpecification(
            amount=task_logic.scalar_value(reward_amount),
            probability=task_logic.scalar_value(p_reward),
            available=task_logic.scalar_value(999999),
            delay=task_logic.scalar_value(0.5),
            operant_logic=task_logic.OperantLogic(
                is_operant=False,
                stop_duration=task_logic.scalar_value(stop_duration),
                time_to_collect_reward=100000,
                grace_distance_threshold=10,
            ),
        ),
        patch_virtual_sites_generator=task_logic.PatchVirtualSitesGenerator(
            inter_patch=task_logic.VirtualSiteGenerator(
                render_specification=task_logic.RenderSpecification(contrast=1),
                label=task_logic.VirtualSiteLabels.INTERPATCH,
                length_distribution=distributions.ExponentialDistribution(
                    distribution_parameters=distributions.ExponentialDistributionParameters(
                        rate=1.0 / inter_patch_mean_length
                    ),
                    scaling_parameters=distributions.ScalingParameters(
                        offset=inter_patch_min_length
                    ),
                    truncation_parameters=distributions.TruncationParameters(
                        min=inter_patch_min_length,
                        max=inter_patch_max_length,
                    ),
                ),
            ),
            inter_site=task_logic.VirtualSiteGenerator(
                render_specification=task_logic.RenderSpecification(contrast=0.5),
                label=task_logic.VirtualSiteLabels.INTERSITE,
                length_distribution=task_logic.scalar_value(inter_site_length),
            ),
            reward_site=task_logic.VirtualSiteGenerator(
                render_specification=task_logic.RenderSpecification(contrast=0.5),
                label=task_logic.VirtualSiteLabels.REWARDSITE,
                length_distribution=task_logic.scalar_value(reward_site_length),
            ),
        ),
    )


def make_block(
    p_c_prime_branch: float = 0.5, noise_std: float = 0.0
) -> task_logic.Block:
    reward_amount = 5.0

    transition_matrix = np.array(
        [
            [0.0, 1.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0 - p_c_prime_branch, p_c_prime_branch, 0.0],
            [0.0, 0.0, 0.0, 0.0, 1.0],
            [1.0, 0.0, 0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0, 0.0, 0.0],
        ]
    )
    if noise_std > 0.0:
        transition_matrix += np.random.normal(0, noise_std, transition_matrix.shape)
        transition_matrix = np.clip(transition_matrix, 0, None)
        transition_matrix = transition_matrix / transition_matrix.sum(
            axis=1, keepdims=True
        )

    return task_logic.Block(
        environment_statistics=task_logic.EnvironmentStatistics(
            patches=[
                make_patch(
                    "A",
                    state_index=0,
                    odor_index=0,
                    p_reward=1.0,
                    reward_amount=reward_amount,
                ),
                make_patch(
                    "B",
                    state_index=1,
                    odor_index=1,
                    p_reward=0.0,
                    reward_amount=reward_amount,
                ),
                make_patch(
                    "B2",
                    state_index=2,
                    odor_index=1,
                    p_reward=0.0,
                    reward_amount=reward_amount,
                ),
                make_patch(
                    "C",
                    state_index=3,
                    odor_index=2,
                    p_reward=0.0,
                    reward_amount=reward_amount,
                ),
                make_patch(
                    "C-prime",
                    state_index=4,
                    odor_index=2,
                    p_reward=1.0,
                    reward_amount=reward_amount,
                ),
            ],
            transition_matrix=transition_matrix.tolist(),
        ),
        end_conditions=[],
    )


def make_operation_control(velocity_threshold: float) -> task_logic.OperationControl:
    return task_logic.OperationControl(
        audio_control=task_logic.AudioControl(duration=0.2, frequency=9999),
        odor_control=task_logic.OdorControl(),
        position_control=task_logic.PositionControl(
            frequency_filter_cutoff=5,
            velocity_threshold=velocity_threshold,
        ),
    )


def make_stage() -> Stage:
    return Stage(
        name="Single Site Graph ABBCC",
        task=task_logic.AindVrForagingTaskLogic(
            task_parameters=task_logic.AindVrForagingTaskParameters(
                rng_seed=None,
                environment=task_logic.BlockStructure(
                    blocks=[make_block(p_c_prime_branch=0.5, noise_std=0.0)]
                ),
                operation_control=make_operation_control(velocity_threshold=5.0),
            ),
        ),
    )


if __name__ == "__main__":
    from pathlib import Path

    stage = make_stage()
    trainer_state = TrainerState[Any](
        curriculum=None, stage=stage, is_on_curriculum=False, active_policies=None
    )
    Path("single_site_graph_definition_stage.json").write_text(
        trainer_state.model_dump_json(indent=4), encoding="utf-8"
    )
