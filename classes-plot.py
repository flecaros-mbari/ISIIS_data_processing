import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def plot_saliency_histogram(csv_path, area_threshold=0):
    df = pd.read_csv(csv_path)
    
    # Filtrar datos por el umbral de área
    df = df[df['saliency'] > area_threshold]
    
    # Configuración de estilo
    sns.set_style("whitegrid")
    
    # Crear figura
    plt.figure(figsize=(10, 6))
    
    # Definir las clases de interés
    classes = ['artifact', 'aggregate', 'particle_blur', 'copepod', 'diatom_chain',
               'phaeocystis', 'larvacean', 'long_particle_blur', 'fecal_pellet',
               'football', 'centric_diatom', 'bubble', 'gelatinous', 
               'chaetognath', 'crustacean']

    for cls in classes:
        subset = df[df['label'] == cls]['saliency']
        print(subset.min(), cls)
        print(sorted(subset))
        if len(subset) == 0:
            continue  # Omitir clases sin datos tras el filtro
        
        counts, bins = np.histogram(subset, bins=200, density=False)  # 20 bins ajustables
        bin_centers = (bins[:-1] + bins[1:]) / 2  # Obtener los centros de los bins
        
        plt.plot(bin_centers, counts, label=cls, linewidth=2)  # Graficar como línea
    
    # Configurar la escala logarítmica en los ejes
    plt.yscale('log')
    plt.xscale('log')
    
    # Etiquetas y título
    plt.xlabel('Area')
    plt.ylabel('Frequency (Log Scale)')
    plt.title(f'Area Distribution by Label (Log Scale, Threshold > {area_threshold})')
    plt.legend(title='Label', loc='upper right', fontsize=8)
    
    plt.show()

# Llamar la función con el threshold deseado
plot_saliency_histogram("/Users/fernandalecaros/Downloads/localizations_with_saliency_area(1).csv", area_threshold=0)
