import threading
import queue
import logging

from simulation_statistics import STATS
from data_structures import ConsensusMessage


class MessageBroker:
    def __init__(self):
        self.drone_inboxes = {}
        self.drones_by_id = {}
        self.lock = threading.Lock()
        logging.info("MessageBroker initialized")

    def register_drone(self, drone_id: str, drone_obj):
        with self.lock:
            self.drone_inboxes[drone_id] = queue.Queue()
            self.drones_by_id[drone_id] = drone_obj
            logging.debug(f"Drone {drone_id} registered")

    def send_message(self, recipient_id: str, message: ConsensusMessage):
        # Incrementa a estatística de mensagens enviadas
        STATS.increment_message_count()

        if recipient_id in self.drone_inboxes:
            self.drone_inboxes[recipient_id].put(message)
            logging.debug(
                f"Broker: Sent {message.msg_type.name} to {recipient_id}"
            )
        else:
            logging.warning(
                f"Broker: Recipient {recipient_id} not found for message "
                f"from {message.sender_id}"
            )

    def broadcast_to_group_candidates(self, group_id: int, sender_id: str,
                                      message: ConsensusMessage):
        logging.debug(
            f"Broker: Broadcasting {message.msg_type.name} from {sender_id} "
            f"to candidates in group {group_id}"
        )

        with self.lock:
            for drone_id, drone_obj in self.drones_by_id.items():
                if drone_obj.group_id == group_id and not \
                   drone_obj.is_management_node and \
                   drone_id != sender_id:

                    self.send_message(drone_id, message)

    def broadcast_to_group_members(self, group_id: int, sender_id: str,
                                   message: ConsensusMessage):
        logging.debug(
            f"Broker: Broadcasting {message.msg_type.name} from "
            f"{sender_id} to members in group {group_id}"
        )

        with self.lock:
            for drone_id, drone_obj in self.drones_by_id.items():
                if drone_obj.group_id == group_id and drone_id != sender_id:
                    self.send_message(drone_id, message)
