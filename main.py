import time
import random
import logging
from collections import defaultdict
from pathlib import Path
from simulation_statistics import STATS
from config import (LOG_DIRECTORY, LOG_FILENAME_PREFIX, LOG_LEVEL, NUM_GROUPS,
                    NUM_DRONES_PER_GROUP, SIMULATION_DURATION_SECONDS,
                    SIMULATION_SPEED_FACTOR)
from data_structures import Position
from broker import MessageBroker
from drone import Drone


def setup_logging():
    """Configura o logger da aplicação."""
    log_dir_path = Path(LOG_DIRECTORY)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_file_timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_filepath = \
        log_dir_path / f"{LOG_FILENAME_PREFIX}_{log_file_timestamp}.log"

    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter(
        '%(asctime)s %(threadName)s %(levelname)s: %(message)s',
        datefmt='%H:%M:%S')

    # Configuração do console
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Configuração de arquivo
    fh = logging.FileHandler(str(log_filepath))
    fh.setLevel(LOG_LEVEL)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logging.info(f"Log file will be saved to: {log_filepath}")
    return log_filepath


def create_drones(broker):
    """
    Cria e registra os drones, retornando a lista de drones e os IDs
    agrupados.
    """
    drones = []
    all_drone_ids_by_group = defaultdict(list)
    drone_id_counter = 1

    for g_id in range(NUM_GROUPS):
        leader_assigned = False
        for _ in range(NUM_DRONES_PER_GROUP):
            drone_id_str = f"D{drone_id_counter}"
            is_leader = not leader_assigned
            leader_assigned = True if not leader_assigned else leader_assigned

            initial_pos = Position(
                random.uniform(0, 5) + g_id * 10,
                random.uniform(0, 5),
                random.uniform(1, 3)
            )

            drone_obj = Drone(
                drone_id_str,
                initial_pos,
                broker,
                group_id=g_id,
                is_management_node=is_leader
            )
            drones.append(drone_obj)
            broker.register_drone(drone_id_str, drone_obj)
            all_drone_ids_by_group[g_id].append(drone_id_str)
            drone_id_counter += 1

    return drones, all_drone_ids_by_group


def assign_management_nodes(drones, all_drone_ids_by_group):
    """
    Atribui os IDs dos membros do grupo e designa o nó líder para cada drone.
    """
    for drone in drones:
        drone.group_members_ids = all_drone_ids_by_group[drone.group_id]
        # Busca o líder do grupo
        for potential_leader in drones:
            if potential_leader.group_id == drone.group_id and \
               potential_leader.is_management_node:
                drone.management_node_id = potential_leader.drone_id
                break

        if not drone.management_node_id and not drone.is_management_node:
            logging.error(
                f"Error: Drone {drone.drone_id} in group {drone.group_id} "
                "could not find its leader. "
                f"Management_node_id is {drone.management_node_id}")
        logging.debug(
            f"Drone {drone.drone_id} members: {drone.group_members_ids}, "
            f"leader: {drone.management_node_id}"
            )


def start_drones(drones):
    """Inicia a execução dos threads dos drones."""
    threads = []
    for drone in drones:
        threads.append(drone)
        drone.start()
    return threads


def run_simulation(drones, threads):
    """Executa a simulação pelos segundos configurados."""
    effective_duration = SIMULATION_DURATION_SECONDS / SIMULATION_SPEED_FACTOR
    logging.info(
        f"--- Simulation starting for {SIMULATION_DURATION_SECONDS:.1f}s "
        f"effective ({effective_duration:.1f}s wall clock) ---"
    )
    start_time = time.time()
    last_log_time = time.time()

    try:
        while time.time() - start_time < effective_duration:
            time.sleep(0.5)
            if time.time() - last_log_time >= 1.0:
                current_sim_time = \
                    (time.time() - start_time) * SIMULATION_SPEED_FACTOR
                scores = {
                    d.drone_id: (
                        f"{d.credit_score:.1f} (P:{d.proposals_made},"
                        f"R:{d.proposals_rejected_by_group})"
                    )
                    for d in drones
                }
                logging.info(
                    f"SimTimeEff: {current_sim_time:.1f}s - "
                    f"Credits (Proposals/Rejections): {scores}"
                )
                last_log_time = time.time()
    except KeyboardInterrupt:
        logging.info("--- Simulation interrupted by user ---")
    finally:
        logging.info("--- Simulation ending, stopping drone threads ---")
        for drone in drones:
            drone.stop()
        for thread in threads:
            thread.join(timeout=2)
        logging.info("--- All drone threads stopped. Simulation finished. ---")
        STATS.print_summary(drones)
        STATS.save_summary_to_json(drones)


if __name__ == "__main__":
    log_file = setup_logging()
    broker = MessageBroker()
    drones, grouped_ids = create_drones(broker)
    assign_management_nodes(drones, grouped_ids)
    threads = start_drones(drones)
    run_simulation(drones, threads)
    logging.info(f"Log file saved to: {log_file}")
