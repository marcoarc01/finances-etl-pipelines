import subprocess
import sys

def run_step(script_name):
    print(f"\n{'='*50}")
    print(f"INICIANDO A ETAPA: {script_name}")
    print(f"{'='*50}")
    
    result = subprocess.run([sys.executable, script_name])
    
    if result.returncode != 0:
        print(f"\nERRO CRÍTICO: A etapa '{script_name}' falhou.")
        print("Pipeline interrompido para evitar dados corrompidos.")
        sys.exit(1)
    else:
        print(f"ETAPA CONCLUÍDA: {script_name} finalizou com sucesso.")

if __name__ == "__main__":
    print("INICIANDO ORQUESTRAÇÃO DO PIPELINE ETL DIÁRIO\n")
    
    try:
        run_step("pipelines/extract.py")
        run_step("pipelines/transform.py")
        run_step("pipelines/load.py")
        
        print("\nPIPELINE DIÁRIO FINALIZADO COM SUCESSO! Banco atualizado.")
        
    except KeyboardInterrupt:
        print("\nPipeline cancelado manualmente pelo usuário.")