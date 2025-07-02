import subprocess
import sys
import time


NUM_RUNS = 10
MAIN_SCRIPT_NAME = "main.py"


def run_simulation(run_number: int, total_runs: int):
    """
    Executa uma única instância do script de simulação e lida com a saída.
    """
    print("-" * 50)
    print(f"--- Iniciando Execução {run_number} de {total_runs} ---")
    print("-" * 50)
    command = [sys.executable, MAIN_SCRIPT_NAME]
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            command, 
            check=True, 
            capture_output=True, 
            text=True,
            encoding='utf-8'
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"--- Execução {run_number} concluída com sucesso em {duration:.2f} segundos. ---")

    except FileNotFoundError:
        print(f"ERRO: O script '{MAIN_SCRIPT_NAME}' não foi encontrado. "
              f"Certifique-se de que este script está na mesma pasta.")
        return False
        
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print("!" * 50)
        print(f"ERRO: A Execução {run_number} falhou após {duration:.2f} segundos.")
        print(f"Código de retorno: {e.returncode}")
        print("--- Saída de Erro (stderr) ---")
        print(e.stderr)
        print("--- Saída Padrão (stdout) ---")
        print(e.stdout)
        print("!" * 50)
        return False
        
    return True


if __name__ == "__main__":
    print(">>> Iniciando conjunto de experimentos...")
    total_start_time = time.time()
    
    successful_runs = 0
    for i in range(NUM_RUNS):
        if run_simulation(run_number=i + 1, total_runs=NUM_RUNS):
            successful_runs += 1
        else:
            print(">>> Conjunto de experimentos interrompido devido a uma falha.")
            break
            
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    print("\n" + "=" * 50)
    print(">>> Conjunto de experimentos finalizado.")
    print(f"Total de execuções bem-sucedidas: {successful_runs} de {NUM_RUNS}")
    print(f"Duração total: {total_duration:.2f} segundos.")
    print("=" * 50)