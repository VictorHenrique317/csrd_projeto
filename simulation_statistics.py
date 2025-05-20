import threading
import time
import logging
import json
from pathlib import Path
from config import (
    SIMULATION_DURATION_SECONDS,
    SIMULATION_SPEED_FACTOR,
    NUM_GROUPS,
    NUM_DRONES_PER_GROUP,
    PROB_IS_INITIALLY_ABS_DEFECTIVE,
    ABS_POS_ERROR_MAGNITUDE,
    PROB_IS_INITIALLY_REL_DEFECTIVE,
    REL_POS_ERROR_MAGNITUDE,
    ABSOLUTE_POSITION_ERROR_THRESHOLD,
    RELATIVE_POSITION_ERROR_THRESHOLD,
    RESULTS_DIRECTORY,
    LOG_FILENAME_PREFIX
)

log_file_timestamp = time.strftime("%Y%m%d-%H%M%S")


class SimulationStatistics:
    def __init__(self):
        self.lock = threading.Lock()
        self.start_real_time = time.time()
        self.total_messages_exchanged = 0
        self.total_completed_steps = 0
        self.consensus_rounds_initiated = 0
        self.consensus_rounds_completed = 0
        self.true_positives = 0
        self.false_positives = 0
        self.true_negatives = 0
        self.false_negatives = 0
        self.prepare_vote_details = []  # List of dicts

        self.consensus_start_times = []
        self.consensus_durations = []

    def increment_message_count(self):
        with self.lock:
            self.total_messages_exchanged += 1

    def increment_completed_steps_count(self):
        with self.lock:
            self.total_completed_steps += 1

    def record_consensus_initiated(self):
        with self.lock:
            self.consensus_rounds_initiated += 1
            # Registra o instante de início deste consenso
            self.consensus_start_times.append(time.time())

    def record_consensus_decision(
        self,
        proposer_id: str,
        seq_num: int,
        proposer_input_was_accurate_gt: bool,
        group_decision_consistent: bool,
    ):
        with self.lock:
            self.consensus_rounds_completed += 1
            # Calcula a duração do consenso se houver um tempo de início
            # registrado
            if self.consensus_start_times:
                start_time = self.consensus_start_times.pop(0)
                duration = time.time() - start_time
                self.consensus_durations.append(duration)
            if proposer_input_was_accurate_gt and group_decision_consistent:
                self.true_positives += 1
            elif (
                not proposer_input_was_accurate_gt
                and group_decision_consistent
            ):
                self.false_positives += 1
                logging.error(
                    "STAT: FALSE POSITIVE! Proposer %s (seq:%s) "
                    "teve entrada INEXATA, mas validada pelo grupo.",
                    proposer_id,
                    seq_num,
                )
            elif (
                not proposer_input_was_accurate_gt
                and not group_decision_consistent
            ):
                self.true_negatives += 1
            elif (
                proposer_input_was_accurate_gt
                and not group_decision_consistent
            ):
                self.false_negatives += 1
                logging.error(
                    "STAT: FALSE NEGATIVE! Proposer %s (seq:%s) teve entrada"
                    " CORRETA, mas não foi validada pelo grupo.",
                    proposer_id,
                    seq_num,
                )

    def record_prepare_phase_voting(
        self,
        seq_num: int,
        proposer_id: str,
        consistent_votes: int,
        total_votes: int,
        group_decision: bool,
    ):
        with self.lock:
            self.prepare_vote_details.append({
                "seq_num": seq_num,
                "proposer_id": proposer_id,
                "consistent_votes_for": consistent_votes,
                "total_prepare_votes_received": total_votes,
                "group_decision_was_consistent": group_decision
            })

    def _calculate_validation_metrics(self):
        vm = {
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "true_negatives": self.true_negatives,
            "false_negatives": self.false_negatives,
        }
        if self.consensus_rounds_completed > 0:
            total = self.consensus_rounds_completed
            vm["accuracy"] = round(
                (self.true_positives + self.true_negatives) / total, 4
            )
            if (self.true_positives + self.false_positives) > 0:
                vm["precision"] = round(
                    self.true_positives / (self.true_positives + self.false_positives), 4  # noqa: E501
                )
            else:
                vm["precision"] = "N/A"
            if (self.true_positives + self.false_negatives) > 0:
                vm["recall"] = round(
                    self.true_positives / (self.true_positives + self.false_negatives), 4  # noqa: E501
                )
            else:
                vm["recall"] = "N/A"
            if (self.true_negatives + self.false_positives) > 0:
                vm["specificity"] = round(
                    self.true_negatives / (self.true_negatives + self.false_positives), 4  # noqa: E501
                )
            else:
                vm["specificity"] = "N/A"
        else:
            vm.update({
                "accuracy": "N/A",
                "precision": "N/A",
                "recall": "N/A",
                "specificity": "N/A"
            })
        return vm

    def _get_summary_data(self, drones_list_objs):
        summary_data = {
            "simulation_parameters": {
                "duration_seconds_effective": SIMULATION_DURATION_SECONDS,
                "speed_factor": SIMULATION_SPEED_FACTOR,
                "num_groups": NUM_GROUPS,
                "num_drones_per_group": NUM_DRONES_PER_GROUP,
                "prob_is_initially_abs_defective": PROB_IS_INITIALLY_ABS_DEFECTIVE,  # noqa: E501
                "abs_pos_error_magnitude": ABS_POS_ERROR_MAGNITUDE,
                "prob_is_initially_rel_defective": PROB_IS_INITIALLY_REL_DEFECTIVE,  # noqa: E501
                "rel_pos_error_magnitude": REL_POS_ERROR_MAGNITUDE,
                "abs_pos_error_threshold_validation": ABSOLUTE_POSITION_ERROR_THRESHOLD,  # noqa: E501
                "rel_pos_error_threshold_validation": RELATIVE_POSITION_ERROR_THRESHOLD,  # noqa: E501
            },
            "overall_performance": {
                "total_messages_exchanged": self.total_messages_exchanged,
                "total_completed_steps": self.total_completed_steps,
                "consensus_rounds_initiated": self.consensus_rounds_initiated,
                "consensus_rounds_completed_with_decision": self.consensus_rounds_completed,  # noqa: E501
                "simulation_wall_clock_time_seconds": round(
                    time.time() - self.start_real_time, 2
                ),
                "average_consensus_time_seconds": round(
                    sum(self.consensus_durations) / len(self.consensus_durations), 4  # noqa: E501
                ) if self.consensus_durations else "N/A"
            },
            "validation_metrics": self._calculate_validation_metrics(),
            "final_drone_info": [
                {
                    "drone_id": drone.drone_id,
                    "group_id": drone.group_id,
                    "final_score": round(drone.credit_score, 1),
                    "is_abs_defective": drone.is_drone_abs_defective,
                    "is_rel_defective": drone.is_drone_rel_defective,
                }
                for drone in sorted(drones_list_objs, key=lambda d: d.drone_id)
            ],
            "drone_proposal_performance": [],
            "prepare_phase_voting_details_sample": self.prepare_vote_details
        }

        for drone in drones_list_objs:
            if drone.proposals_made > 0:
                rejection_rate = (
                    drone.proposals_rejected_by_group / drone.proposals_made * 100  # noqa: E501
                )
                success_rate = (
                    (drone.proposals_made - drone.proposals_rejected_by_group)
                    / drone.proposals_made * 100
                )
            else:
                rejection_rate = success_rate = 0

            summary_data["drone_proposal_performance"].append({
                "drone_id": drone.drone_id,
                "proposals_made": drone.proposals_made,
                "proposals_rejected": drone.proposals_rejected_by_group,
                "proposals_successful": drone.proposals_made - drone.proposals_rejected_by_group,  # noqa: E501
                "rejection_rate_percent": round(rejection_rate, 1),
                "success_rate_percent": round(success_rate, 1)
            })

        summary_data["drone_proposal_performance"].sort(
            key=lambda x: x["rejection_rate_percent"], reverse=True
        )

        return summary_data

    def _format_percentage(self, value):
        return f"{value:.2%}" if isinstance(value, float) else value

    def print_summary(self, drones_list_objs):
        summary = self._get_summary_data(drones_list_objs)

        # Cabeçalho dos dados gerais
        logging.critical("--- SIMULATION STATISTICS (CONSOLE) ---")
        op = summary["overall_performance"]
        logging.critical(
            "Total Messages Exchanged: %s", op["total_messages_exchanged"]
        )
        logging.critical(
            "Total Completed Steps: %s", op["total_completed_steps"]
        )
        logging.critical(
            "Consensus Rounds Initiated: %s", op["consensus_rounds_initiated"]
        )
        logging.critical(
            "Consensus Rounds Completed: %s",
            op["consensus_rounds_completed_with_decision"]
        )
        logging.critical(
            "Simulation Wall Clock Time: %ss",
            op["simulation_wall_clock_time_seconds"]
        )
        logging.critical(
            "Average Consensus Time: %s seconds",
            op["average_consensus_time_seconds"]
        )
        logging.critical("-" * 30)

        # Métricas de validação
        vm = summary["validation_metrics"]
        logging.critical(
            "Validation Performance (baseado na precisão do sensor do propositor):"  # noqa: E501
        )
        logging.critical("  True Positives: %s", vm["true_positives"])
        logging.critical("  False Positives: %s << SYSTEM ERROR", vm["false_positives"])  # noqa: E501
        logging.critical("  True Negatives: %s", vm["true_negatives"])
        logging.critical("  False Negatives: %s << SYSTEM ERROR", vm["false_negatives"])  # noqa: E501
        logging.critical("  Overall Accuracy: %s", self._format_percentage(vm["accuracy"]))  # noqa: E501
        logging.critical("  Precision: %s", self._format_percentage(vm["precision"]))  # noqa: E501
        logging.critical("  Recall: %s", self._format_percentage(vm["recall"]))
        logging.critical("  Specificity: %s", self._format_percentage(vm["specificity"]))  # noqa: E501
        logging.critical("-" * 30)

        # Informações finais dos drones
        logging.critical("Final Drone informations:")
        for info in summary["final_drone_info"]:
            logging.critical(
                "  Drone id:%s, Group id:%s, Score: %s, Is abs defective?: %s, Is rel defective?: %s",  # noqa: E501
                info["drone_id"], info["group_id"], info["final_score"],
                info["is_abs_defective"], info["is_rel_defective"]
            )
        logging.critical("-" * 30)

        # Desempenho das propostas dos drones
        logging.critical("Drone Proposal Performance (Ordenado por Taxa de Rejeição):")  # noqa: E501
        for dp in summary["drone_proposal_performance"]:
            logging.critical(
                "    %s: %s/%s rejeitados (%s%%)",
                dp["drone_id"],
                dp["proposals_rejected"],
                dp["proposals_made"],
                dp["rejection_rate_percent"],
            )
        logging.critical("--- END OF CONSOLE STATISTICS ---")

    def save_summary_to_json(self, drones_list_objs):
        """Salva as estatísticas em um arquivo JSON."""
        summary = self._get_summary_data(drones_list_objs)
        results_dir = Path(RESULTS_DIRECTORY)
        results_dir.mkdir(parents=True, exist_ok=True)

        json_filename = (
            f"{LOG_FILENAME_PREFIX}_{log_file_timestamp}_summary.json"
        )
        json_filepath = results_dir / json_filename

        try:
            with open(json_filepath, "w") as f:
                json.dump(summary, f, indent=4)
            logging.info("Simulation summary saved to: %s", json_filepath)
        except Exception as e:
            logging.error(
                "Failed to save summary JSON to %s: %s", json_filepath, e
            )


STATS = SimulationStatistics()
