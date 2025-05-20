import threading
import time
import random
import queue
import math
import logging

from simulation_statistics import STATS
from config import (
    SIMULATION_SPEED_FACTOR, PROB_IS_INITIALLY_ABS_DEFECTIVE,
    ABS_POS_ERROR_MAGNITUDE, PROB_IS_INITIALLY_REL_DEFECTIVE,
    REL_POS_ERROR_MAGNITUDE, RELATIVE_POSITION_ERROR_THRESHOLD,
    ABSOLUTE_POSITION_ERROR_THRESHOLD, VOTING_THRESHOLD_PREPARES,
    VOTING_THRESHOLD_CONSISTENCY_IN_PREPARES,
    CREDIT_MANAGED_SUCCESSFUL_VALIDATION, CREDIT_SUCCESSFUL_PROPOSAL_VALIDATED,
    CREDIT_MANAGED_REJECTION, CREDIT_REJECTED_PROPOSAL,
    CREDIT_PARTICIPATED_SUCCESSFUL_VALIDATION, CREDIT_PARTICIPATED_REJECTION,
    PROB_DRONE_INITIATES_CONSENSUS, PROB_LEADER_INITIATES_CONSENSUS_SELF,
    PROB_ABS_GETS_DEFECTIVE, PROB_REL_GETS_DEFECTIVE,
    SIMULATION_DURATION_SECONDS)
from data_structures import (
    Position, DroneConsensusStatus, DistanceVector, calculate_vector_distance,
    calculate_distance, ConsensusMessage, MessageType)
from broker import MessageBroker


class Drone(threading.Thread):
    def __init__(
            self,
            drone_id: str,
            initial_pos: Position,
            broker: MessageBroker,
            group_id: int,
            is_management_node: bool = False):
        super().__init__(name=f"Drone-{drone_id}")
        self.drone_id = drone_id
        self.true_abs_pos = \
            Position(initial_pos.x, initial_pos.y, initial_pos.z)
        self.sensed_abs_pos = \
            Position(initial_pos.x, initial_pos.y, initial_pos.z)
        self.was_last_abs_pos_reading_intentionally_bad = False
        self.credit_score = 70.0
        self.message_broker = broker
        self.message_inbox = broker.drone_inboxes.get(drone_id)
        self.group_id = group_id
        self.is_management_node = is_management_node
        self.group_members_ids = []
        self.management_node_id = None
        self.consensus_status = DroneConsensusStatus.IDLE
        self.current_view = 0
        self.current_seq_num = 0
        self.active_consensus_proposals = {}
        self.stop_event = threading.Event()
        self.proposals_made = 0
        self.proposals_rejected_by_group = 0
        self.is_drone_abs_defective = random.random() < PROB_IS_INITIALLY_ABS_DEFECTIVE  # noqa: E501
        self.is_drone_rel_defective = random.random() < PROB_IS_INITIALLY_REL_DEFECTIVE  # noqa: E501

        logging.info(
            f"Drone {self.drone_id} initialized at true_pos {self.true_abs_pos}, "  # noqa: E501
            f"Group: {self.group_id}, Leader: {self.is_management_node}"
        )

    def _get_group_candidate_ids(self):
        candidates = []
        for mid in self.group_members_ids:
            if mid == self.management_node_id or mid == self.drone_id:
                continue
            drone_obj = self.message_broker.drones_by_id.get(mid)
            if drone_obj and drone_obj.credit_score >= 40:
                candidates.append(mid)
        return candidates

    def _get_group_management_node_id(self):
        return self.management_node_id

    def _calculate_digest(self, t) -> str:
        return str(hash(t))

    def _sign_message(self, m: ConsensusMessage) -> str:
        if not m.digest:
            digest_content_tuple = (
                m.original_proposer_id,
                m.proposed_abs_pos.x if m.proposed_abs_pos else None,
                m.proposed_abs_pos.y if m.proposed_abs_pos else None,
                m.proposed_abs_pos.z if m.proposed_abs_pos else None,
                tuple(sorted([
                    (k, (v.x, v.y, v.z))
                    for k, v in m.proposed_rel_meas.items()
                ])) if m.proposed_rel_meas else None,
                m.timestamp,
                m.view_id,
                m.seq_num
            )
            m.digest = self._calculate_digest(digest_content_tuple)
        return f"sig_{self.drone_id}_{m.digest}"

    def _verify_signature(self, m: ConsensusMessage) -> bool:
        return True

    def _update_credit_score(self, adj: float, reason: str):
        old = self.credit_score
        self.credit_score = max(0, min(100, self.credit_score + adj))
        logging.info(
            f"Credit for {self.drone_id} {old:.1f} -> {self.credit_score:.1f}"
            f" due to: {reason}"
        )

    def _simulate_movement_and_sensor_update(self):
        self.true_abs_pos.x += \
            random.uniform(-0.1, 0.1) * SIMULATION_SPEED_FACTOR
        self.true_abs_pos.y += \
            random.uniform(-0.1, 0.1) * SIMULATION_SPEED_FACTOR

        self.was_last_abs_pos_reading_intentionally_bad = False
        if self.is_drone_abs_defective:
            self.was_last_abs_pos_reading_intentionally_bad = True
            err_x = random.uniform(-ABS_POS_ERROR_MAGNITUDE,
                                   ABS_POS_ERROR_MAGNITUDE)
            err_y = random.uniform(-ABS_POS_ERROR_MAGNITUDE,
                                   ABS_POS_ERROR_MAGNITUDE)
            self.sensed_abs_pos = Position(
                self.true_abs_pos.x + err_x,
                self.true_abs_pos.y + err_y,
                self.true_abs_pos.z
            )
            logging.warning(
                f"{self.drone_id} ABS POS SENSOR ERROR INJECTED. "
                f"True:{self.true_abs_pos}, "
                f"Sensed:{self.sensed_abs_pos}"
            )
        else:
            self.sensed_abs_pos = Position(
                self.true_abs_pos.x,
                self.true_abs_pos.y,
                self.true_abs_pos.z
            )

    def get_sensed_absolute_position(self) -> Position:
        self._simulate_movement_and_sensor_update()
        return self.sensed_abs_pos

    def get_current_relative_measurements(self) -> dict:
        measurements = {}
        for member_id in self.group_members_ids:
            if member_id != self.drone_id:
                member_drone_obj = \
                    self.message_broker.drones_by_id.get(member_id)
                if member_drone_obj:
                    true_vec = \
                        member_drone_obj.true_abs_pos - self.true_abs_pos
                    sensed_vec = DistanceVector(true_vec.x, true_vec.y,
                                                true_vec.z)
                    if self.is_drone_rel_defective:
                        err_dx = random.uniform(
                            -REL_POS_ERROR_MAGNITUDE, REL_POS_ERROR_MAGNITUDE
                        )
                        err_dy = random.uniform(
                            -REL_POS_ERROR_MAGNITUDE, REL_POS_ERROR_MAGNITUDE
                        )
                        sensed_vec.x += err_dx
                        sensed_vec.y += err_dy
                        logging.warning(
                            f"{self.drone_id} REL POS SENSOR ERROR INJECTED "
                            f"measuring {member_id}. "
                            f"TrueVec:{true_vec}, SensedVec:{sensed_vec}"
                        )
                    measurements[member_id] = sensed_vec
        return measurements

    def initiate_position_consensus(self):
        if self.consensus_status != DroneConsensusStatus.IDLE:
            return

        STATS.record_consensus_initiated()
        self.proposals_made += 1
        logging.info(
            f"{self.drone_id} initiating position consensus "
            f"(Total proposals: {self.proposals_made})."
        )
        self.consensus_status = DroneConsensusStatus.AWAITING_LEADER_RESPONSE

        current_sensed_abs_pos = self.get_sensed_absolute_position()
        proposer_input_was_accurate_gt = not \
            self.was_last_abs_pos_reading_intentionally_bad
        current_rel_meas = self.get_current_relative_measurements()
        leader_id = self._get_group_management_node_id()

        if not leader_id:
            logging.error(
                f"{self.drone_id}: Could not find leader for group "
                f"{self.group_id}"
            )
            self.consensus_status = DroneConsensusStatus.IDLE
            return

        msg = ConsensusMessage(
            msg_type=MessageType.REQUEST_POSITION_CONSENSUS,
            sender_id=self.drone_id,
            original_proposer_id=self.drone_id,
            proposed_abs_pos=current_sensed_abs_pos,
            proposed_rel_meas=current_rel_meas,
            timestamp=time.time(),
            proposer_input_was_accurate_gt=proposer_input_was_accurate_gt
        )
        self.message_broker.send_message(leader_id, msg)
        logging.info(
            f"{self.drone_id} sent REQUEST to leader {leader_id} for "
            f"sensed_pos {current_sensed_abs_pos} "
            f"(Input Accurate GT: {proposer_input_was_accurate_gt})"
        )

    def _handle_request_position_consensus(self, message: ConsensusMessage):
        if not self.is_management_node:
            return

        logging.info(
            f"Leader {self.drone_id} received REQUEST from "
            f"{message.original_proposer_id} (proposing "
            f"{message.proposed_abs_pos}, InputAccGT: "
            f"{message.proposer_input_was_accurate_gt})"
        )
        proposal_key = (self.current_view, self.current_seq_num)
        if proposal_key in self.active_consensus_proposals:
            logging.warning(
                f"Leader {self.drone_id} busy with {proposal_key}, "
                f"ignoring new request"
            )
            return

        self.active_consensus_proposals[proposal_key] = {
            "original_proposer_id": message.original_proposer_id,
            "proposed_abs_pos": message.proposed_abs_pos,
            "proposed_rel_meas": message.proposed_rel_meas,
            "timestamp": message.timestamp,
            "proposer_input_was_accurate_gt": message.proposer_input_was_accurate_gt,  # noqa: E501
            "intra_prepare_votes": {},
            "intra_commit_votes": {}
        }

        digest_content = (
            message.original_proposer_id,
            message.proposed_abs_pos.x,
            message.proposed_abs_pos.y,
            message.proposed_abs_pos.z,
            tuple(sorted([
                (k, (v.x, v.y, v.z))
                for k, v in message.proposed_rel_meas.items()
            ])),
            message.timestamp,
            self.current_view,
            self.current_seq_num
        )
        digest_val = self._calculate_digest(digest_content)
        pre_prepare_msg = ConsensusMessage(
            msg_type=MessageType.PRE_PREPARE,
            sender_id=self.drone_id,
            original_proposer_id=message.original_proposer_id,
            proposed_abs_pos=message.proposed_abs_pos,
            proposed_rel_meas=message.proposed_rel_meas,
            timestamp=message.timestamp,
            view_id=self.current_view,
            seq_num=self.current_seq_num,
            digest=digest_val,
            proposer_input_was_accurate_gt=message.proposer_input_was_accurate_gt  # noqa: E501
        )
        pre_prepare_msg.signature = self._sign_message(pre_prepare_msg)
        self.message_broker.broadcast_to_group_candidates(
            self.group_id, self.drone_id, pre_prepare_msg
        )
        logging.info(
            f"Leader {self.drone_id} sent PRE_PREPARE "
            f"(seq:{self.current_seq_num}) for {message.original_proposer_id}"
        )
        self.consensus_status = DroneConsensusStatus.AWAITING_INTRA_PREPARES

    def _handle_pre_prepare(self, message: ConsensusMessage):
        if self.is_management_node:
            return

        if self.credit_score < 40:
            return

        logging.info(
            f"Candidate {self.drone_id} received PRE_PREPARE "
            f"from {message.sender_id} for {message.original_proposer_id} "
            f"(pos: {message.proposed_abs_pos}, InputAccGT: "
            f"{message.proposer_input_was_accurate_gt})"
        )
        self.consensus_status = DroneConsensusStatus.PROCESSING_PRE_PREPARE
        self.active_consensus_proposals[(message.view_id, message.seq_num)] = {
            "original_proposer_id": message.original_proposer_id,
            "proposed_abs_pos": message.proposed_abs_pos,
            "proposed_rel_meas": message.proposed_rel_meas,
            "timestamp": message.timestamp,
            "digest": message.digest,
            "proposer_input_was_accurate_gt":
                message.proposer_input_was_accurate_gt
        }
        proposer_A_id = message.original_proposer_id
        proposed_abs_pos_A = message.proposed_abs_pos
        proposed_rel_meas_by_A = message.proposed_rel_meas

        local_consistency_flag = True
        my_id_as_str = str(self.drone_id)

        if my_id_as_str in proposed_rel_meas_by_A:
            vec_A_to_me = proposed_rel_meas_by_A[my_id_as_str]
            my_meas_of_A = self.get_current_relative_measurements() \
                .get(proposer_A_id)
            if my_meas_of_A:
                if (calculate_vector_distance(vec_A_to_me, -my_meas_of_A)
                        > RELATIVE_POSITION_ERROR_THRESHOLD):
                    local_consistency_flag = False
                    logging.warning(
                        f"{self.drone_id} (validating {proposer_A_id}): "
                        f"Rel. meas inconsistency. A's_meas_of_me:{vec_A_to_me},"  # noqa: E501
                        f" My_meas_of_A(neg):{-my_meas_of_A}"
                    )
            else:
                logging.debug(
                    f"{self.drone_id} (validating {proposer_A_id}): "
                    f"Cannot verify A's rel. meas of me, "
                    f"no current meas. for {proposer_A_id}"
                )
        else:
            logging.debug(
                f"{self.drone_id} (validating {proposer_A_id}): "
                f"Proposer {proposer_A_id} did not provide rel. meas. "
                f"for me ({my_id_as_str})."
            )

        if local_consistency_flag:
            my_sensed_abs_pos = self.get_sensed_absolute_position()
            my_rel_meas_of_A = self.get_current_relative_measurements() \
                .get(proposer_A_id)
            if my_rel_meas_of_A:
                est_abs_pos_A = my_sensed_abs_pos + my_rel_meas_of_A
                if calculate_distance(proposed_abs_pos_A, est_abs_pos_A) > \
                        ABSOLUTE_POSITION_ERROR_THRESHOLD:
                    local_consistency_flag = False
                    logging.warning(
                        f"{self.drone_id} (validating {proposer_A_id}): Abs."
                        f" pos inconsistency. A_proposed:{proposed_abs_pos_A}, "  # noqa: E501
                        f"My_estimate_of_A:{est_abs_pos_A}"
                    )
            else:
                logging.warning(
                    f"{self.drone_id} (validating {proposer_A_id}): "
                    f"Cannot robustly verify abs. pos of {proposer_A_id}, "
                    f"no current rel. meas. for {proposer_A_id}."
                )
        logging.info(
            f"Candidate {self.drone_id} determined local consistency "
            f"for {proposer_A_id} (seq:{message.seq_num}) "
            f"as {local_consistency_flag}"
        )
        intra_prepare_msg = ConsensusMessage(
            msg_type=MessageType.INTRA_PREPARE,
            sender_id=self.drone_id,
            original_proposer_id=message.original_proposer_id,
            view_id=message.view_id,
            seq_num=message.seq_num,
            digest=message.digest,
            consistency_flag=local_consistency_flag,
            proposer_input_was_accurate_gt=message.proposer_input_was_accurate_gt  # noqa: E501
        )
        intra_prepare_msg.signature = self._sign_message(intra_prepare_msg)
        leader_id = self._get_group_management_node_id()
        self.message_broker.send_message(leader_id, intra_prepare_msg)
        logging.info(
            f"Candidate {self.drone_id} sent INTRA_PREPARE "
            f"(seq:{message.seq_num}) to leader {leader_id} with flag "
            f"{local_consistency_flag}"
        )
        self.consensus_status = DroneConsensusStatus.AWAITING_INTRA_COMMITS

    def _handle_intra_prepare(self, message: ConsensusMessage):
        if not self.is_management_node:
            return

        logging.info(
            f"Leader {self.drone_id} received INTRA_PREPARE from "
            f"{message.sender_id} (seq:{message.seq_num}) "
            f"flag:{message.consistency_flag} "
            f"(InputAccGT: {message.proposer_input_was_accurate_gt})"
        )
        proposal_key = (message.view_id, message.seq_num)
        if proposal_key not in self.active_consensus_proposals:
            logging.warning(
                f"Leader {self.drone_id}: Received INTRA_PREPARE for unknown "
                f"proposal {proposal_key}. Ignoring."
            )
            return

        current_proposal = self.active_consensus_proposals[proposal_key]
        current_proposal["intra_prepare_votes"][message.sender_id] = (
            message.consistency_flag
        )
        if "proposer_input_was_accurate_gt" not in current_proposal:
            current_proposal["proposer_input_was_accurate_gt"] = (
                message.proposer_input_was_accurate_gt
            )

        num_candidates = len(self._get_group_candidate_ids())
        min_prepares_to_proceed = max(
            1, math.ceil(num_candidates * VOTING_THRESHOLD_PREPARES)
        )

        if len(current_proposal["intra_prepare_votes"]) >= min_prepares_to_proceed:  # noqa: E501
            consistent_votes_count = sum(
                1 for f in current_proposal["intra_prepare_votes"].values()
                if f is True
            )
            total_votes_received = len(current_proposal["intra_prepare_votes"])
            min_consistent_for_group_ok = math.ceil(
                total_votes_received * VOTING_THRESHOLD_CONSISTENCY_IN_PREPARES
            )
            group_decision_flag = (
                consistent_votes_count >= min_consistent_for_group_ok
            )

            logging.info(
                f"Leader {self.drone_id} (seq:{message.seq_num}): PREPARE "
                f"phase complete. Votes: "
                f"{current_proposal['intra_prepare_votes']}. Consistent: "
                f"{consistent_votes_count}/{total_votes_received}. "
                f"Group Decision: {group_decision_flag}"
            )
            STATS.record_prepare_phase_voting(
                message.seq_num,
                current_proposal["original_proposer_id"],
                consistent_votes_count,
                total_votes_received,
                group_decision_flag
            )
            current_proposal["group_decision_flag"] = group_decision_flag
            intra_commit_msg = ConsensusMessage(
                msg_type=MessageType.INTRA_COMMIT,
                sender_id=self.drone_id,
                original_proposer_id=current_proposal["original_proposer_id"],
                proposed_abs_pos=current_proposal["proposed_abs_pos"],
                view_id=message.view_id,
                seq_num=message.seq_num,
                digest=message.digest,
                group_decision_flag=group_decision_flag,
                proposer_input_was_accurate_gt=current_proposal["proposer_input_was_accurate_gt"]  # noqa: E501
            )
            intra_commit_msg.signature = self._sign_message(intra_commit_msg)
            self.message_broker.broadcast_to_group_members(
                self.group_id, self.drone_id, intra_commit_msg
            )
            logging.info(
                f"Leader {self.drone_id} sent INTRA_COMMIT "
                f"(seq:{message.seq_num}) with group_decision "
                f"{group_decision_flag}"
            )
            self._handle_intra_commit(intra_commit_msg, is_self_leader=True)
            self.consensus_status = DroneConsensusStatus.AWAITING_INTRA_COMMITS

    def _handle_intra_commit(self,
                             message: ConsensusMessage,
                             is_self_leader=False):
        log_prefix = "Leader" if self.is_management_node and is_self_leader \
            else "Candidate"
        logging.info(
            f"{log_prefix} {self.drone_id} received INTRA_COMMIT "
            f"from {message.sender_id} (seq:{message.seq_num}) "
            f"group_decision:{message.group_decision_flag} "
            f"(InputAccGT: {message.proposer_input_was_accurate_gt})"
        )
        proposal_key = (message.view_id, message.seq_num)
        if proposal_key not in self.active_consensus_proposals:
            logging.warning(
                f"{log_prefix} {self.drone_id}: Received INTRA_COMMIT for "
                f"unknown proposal {proposal_key}. Ignoring."
            )
            return

        current_proposal_info = self.active_consensus_proposals[proposal_key]
        original_proposer_id = current_proposal_info["original_proposer_id"]
        proposer_drone_obj = self.message_broker.drones_by_id \
            .get(original_proposer_id)
        proposer_input_accurate_gt = message.proposer_input_was_accurate_gt

        if proposer_input_accurate_gt is None:
            logging.error(
                f"CRITICAL: Ground truth missing for proposal {proposal_key} "
                f"in _handle_intra_commit by {self.drone_id} from "
                f"{message.sender_id}"
            )
            proposer_input_accurate_gt = True

        if self.is_management_node and is_self_leader:
            STATS.record_consensus_decision(
                original_proposer_id,
                message.seq_num,
                proposer_input_accurate_gt,
                message.group_decision_flag
            )
            if message.group_decision_flag:
                logging.critical(
                    f"Leader {self.drone_id}: CONSENSUS SUCCEEDED for "
                    f"{original_proposer_id} (seq:{message.seq_num}) Pos:"
                    f"{current_proposal_info['proposed_abs_pos']}. GroupOK."
                )
                self._update_credit_score(
                    CREDIT_MANAGED_SUCCESSFUL_VALIDATION,
                    f"Managed valid {original_proposer_id} "
                    f"(seq {message.seq_num})"
                )
                if proposer_drone_obj:
                    proposer_drone_obj._update_credit_score(
                        CREDIT_SUCCESSFUL_PROPOSAL_VALIDATED,
                        f"My pos validated (seq {message.seq_num})"
                    )
            else:
                logging.critical(
                    f"Leader {self.drone_id}: CONSENSUS: Position for "
                    f"{original_proposer_id} (seq:{message.seq_num}) REJECTED. "  # noqa: E501
                    f"Proposed:{current_proposal_info['proposed_abs_pos']}"
                )
                self._update_credit_score(
                    CREDIT_MANAGED_REJECTION,
                    f"Managed rejected {original_proposer_id} "
                    f"(seq {message.seq_num})"
                )
                if proposer_drone_obj:
                    proposer_drone_obj.proposals_rejected_by_group += 1
                    proposer_drone_obj._update_credit_score(
                        CREDIT_REJECTED_PROPOSAL,
                        f"My pos rejected (seq {message.seq_num})"
                    )
            del self.active_consensus_proposals[proposal_key]
            self.current_seq_num += 1
            self.consensus_status = DroneConsensusStatus.IDLE
        elif not self.is_management_node:
            if message.group_decision_flag:
                logging.info(
                    f"Candidate {self.drone_id}: Accepted group OK for "
                    f"{original_proposer_id} (seq:{message.seq_num}). "
                    f"Pos:{current_proposal_info['proposed_abs_pos']}"
                )
                self._update_credit_score(
                    CREDIT_PARTICIPATED_SUCCESSFUL_VALIDATION,
                    f"Participated valid {original_proposer_id} "
                    f"(seq {message.seq_num})"
                )
            else:
                logging.info(
                    f"Candidate {self.drone_id}: Accepted group REJECTION for "
                    f"{original_proposer_id} (seq:{message.seq_num})."
                )
                self._update_credit_score(
                    CREDIT_PARTICIPATED_REJECTION,
                    f"Participated rejected {original_proposer_id} "
                    f"(seq {message.seq_num})"
                )
            if proposal_key in self.active_consensus_proposals:
                del self.active_consensus_proposals[proposal_key]
            self.consensus_status = DroneConsensusStatus.IDLE

    def _randomly_introduce_defect(self):
        delta_t = 0.05 * SIMULATION_SPEED_FACTOR
        p_abs_per_step = PROB_ABS_GETS_DEFECTIVE * delta_t
        p_rel_per_step = PROB_REL_GETS_DEFECTIVE * delta_t
        if random.random() < p_abs_per_step:
            self.is_drone_abs_defective = True
            logging.info(
                f"{self.drone_id} ABS DEFECTIVE SENSOR INJECTED."
            )

        if random.random() < p_rel_per_step:
            self.is_drone_rel_defective = True
            logging.info(
                f"{self.drone_id} REL DEFECTIVE SENSOR INJECTED."
            )

    def run(self):
        logging.info(f"Drone {self.drone_id} thread started.")
        self.message_inbox = self.message_broker.drone_inboxes \
            .get(self.drone_id)
        while not self.stop_event.is_set():
            try:
                message = self.message_inbox.get(
                    timeout=0.1 * SIMULATION_SPEED_FACTOR
                )
                if not self._verify_signature(message):
                    logging.warning(
                        f"{self.drone_id} invalid signature from "
                        f"{message.sender_id}. Discarding."
                    )
                    continue
                logging.debug(f"{self.drone_id} received: {message}")
                if (message.msg_type == MessageType.REQUEST_POSITION_CONSENSUS and  # noqa: E501
                        self.is_management_node):
                    # Drone de gerenciamento recebe requisição de consenso
                    self._handle_request_position_consensus(message)
                elif (message.msg_type == MessageType.PRE_PREPARE and
                      not self.is_management_node):
                    # Candidatos recebem PRE_PREPARE do líder
                    self._handle_pre_prepare(message)
                elif (message.msg_type == MessageType.INTRA_PREPARE and
                      self.is_management_node):
                    # Drone de gerenciamento recebe INTRA_PREPARE
                    # dos candidatos
                    self._handle_intra_prepare(message)
                elif message.msg_type == MessageType.INTRA_COMMIT:
                    # Todos os drones recebem INTRA_COMMIT com o
                    # resultado do consenso
                    self._handle_intra_commit(message, is_self_leader=False)
                else:
                    logging.debug(
                        f"{self.drone_id} unhandled message type: "
                        f"{message.msg_type}"
                    )
            except queue.Empty:
                if self.consensus_status == DroneConsensusStatus.IDLE:
                    prob_init = (
                        PROB_DRONE_INITIATES_CONSENSUS /
                        (len(self.group_members_ids) or 1)
                    )
                    if self.is_management_node:
                        prob_init = PROB_LEADER_INITIATES_CONSENSUS_SELF
                    if random.random() < prob_init:
                        self.initiate_position_consensus()
            except Exception as e:
                logging.error(
                    f"Error in drone {self.drone_id} run loop: {e}",
                    exc_info=True
                )

            self._randomly_introduce_defect()
            STATS.increment_completed_steps_count()
            time.sleep(0.05 * SIMULATION_SPEED_FACTOR)
        logging.info(f"Drone {self.drone_id} thread stopped.")

    def stop(self):
        self.stop_event.set()
