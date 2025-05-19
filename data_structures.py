import time
import math
from enum import Enum, auto


class MessageType(Enum):
    REQUEST_POSITION_CONSENSUS = auto()
    PRE_PREPARE = auto()
    INTRA_PREPARE = auto()
    INTRA_COMMIT = auto()


class DroneConsensusStatus(Enum):
    IDLE = auto()
    AWAITING_LEADER_RESPONSE = auto()
    AWAITING_INTRA_PREPARES = auto()
    AWAITING_INTRA_COMMITS = auto()
    PROCESSING_PRE_PREPARE = auto()
    PROCESSING_INTRA_COMMIT = auto()


class Position:
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z

    def __repr__(self):
        return f"Pos(x={self.x:.2f}, y={self.y:.2f}, z={self.z:.2f})"

    def __add__(self, other):
        return Position(self.x + other.x, self.y + other.y, self.z + other.z)

    def __sub__(self, other):
        return DistanceVector(
            self.x - other.x, self.y - other.y, self.z - other.z
        )


class DistanceVector:
    def __init__(self, dx, dy, dz):
        self.x = dx
        self.y = dy
        self.z = dz

    def __repr__(self):
        return f"Vec(dx={self.x:.2f}, dy={self.y:.2f}, dz={self.z:.2f})"

    def __neg__(self):
        return DistanceVector(-self.x, -self.y, -self.z)

    def magnitude(self):
        return math.sqrt(self.x ** 2 + self.y ** 2 + self.z ** 2)


def calculate_distance(p1: Position, p2: Position) -> float:
    if p1 is None or p2 is None:
        return float('inf')
    return math.sqrt(
        (p1.x - p2.x) ** 2 + (p1.y - p2.y) ** 2 + (p1.z - p2.z) ** 2
    )


def calculate_vector_distance(v1: DistanceVector, v2: DistanceVector) -> float:
    if v1 is None or v2 is None:
        return float('inf')
    return math.sqrt(
        (v1.x - v2.x) ** 2 + (v1.y - v2.y) ** 2 + (v1.z - v2.z) ** 2
    )


class ConsensusMessage:
    def __init__(self,
                 msg_type: MessageType,
                 sender_id: str,
                 original_proposer_id: str = None,
                 proposed_abs_pos: Position = None,
                 proposed_rel_meas: dict = None,
                 timestamp: float = None,
                 view_id: int = 0,
                 seq_num: int = 0,
                 digest: str = None,
                 consistency_flag: bool = None,
                 signature: str = None,
                 group_decision_flag: bool = None,
                 proposer_input_was_accurate_gt: bool = None):
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.original_proposer_id = original_proposer_id
        self.proposed_abs_pos = proposed_abs_pos
        self.proposed_rel_meas = proposed_rel_meas if proposed_rel_meas else {}
        self.timestamp = timestamp if timestamp else time.time()
        self.view_id = view_id
        self.seq_num = seq_num
        self.digest = digest
        self.consistency_flag = consistency_flag
        self.signature = signature
        self.group_decision_flag = group_decision_flag
        self.proposer_input_was_accurate_gt = proposer_input_was_accurate_gt

    def __repr__(self):
        gt_info = f" GT_Acc:{self.proposer_input_was_accurate_gt}" if \
            self.proposer_input_was_accurate_gt is not None else ""
        return (
            f"Msg({self.msg_type.name} from {self.sender_id} for "
            f"{self.original_proposer_id} "
            f"Seq:{self.seq_num} V:{self.view_id} Pos:{self.proposed_abs_pos} "
            f"CFlag:{self.consistency_flag} "
            f"GFlag:{self.group_decision_flag}{gt_info})"
        )
