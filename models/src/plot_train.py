import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    plt.style.use('ggplot')
    log_path = "c:/Users/keyle/OneDrive/Documentos/GitHub/precipitacion-analisis/models/logs/experiment_log_20250602_223623.txt"
    df = pd.read_csv(log_path, sep='\t')
    fig, axs = plt.subplots(1, 2, figsize=(14, 5))
    axs[0].plot(df['Epoch'], df['Loss'], label='Train Loss')
    axs[0].plot(df['Epoch'], df['Val_Loss'], label='Validation Loss')
    axs[0].set_xlabel('Epoch')
    axs[0].set_ylabel('Loss')
    axs[0].set_title('Loss')
    axs[0].legend()
    axs[0].grid(True)
    axs[1].plot(df['Epoch'], df['MAE'], label='Train MAE')
    axs[1].plot(df['Epoch'], df['Val_MAE'], label='Validation MAE')
    axs[1].set_xlabel('Epoch')
    axs[1].set_ylabel('MAE')
    axs[1].set_title('MAE')
    axs[1].legend()
    axs[1].grid(True)

    plt.tight_layout()
    plt.show()
    fig.savefig('c:/Users/keyle/OneDrive/Documentos/GitHub/precipitacion-analisis/models/imgs/training_metrics.png', dpi=500, bbox_inches='tight')