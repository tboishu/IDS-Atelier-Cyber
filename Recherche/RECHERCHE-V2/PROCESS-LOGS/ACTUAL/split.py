import os

def split_csv_by_size(input_file, output_dir="divided", max_size_mb=10):
    """
    Divise un fichier CSV en plusieurs fichiers d'environ max_size_mb Mo chacun,
    sans modifier le contenu et en conservant l'en-tête dans chaque fichier.
    """
    # Conversion en octets
    max_size_bytes = max_size_mb * 1024 * 1024

    # Créer le dossier de sortie si inexistant
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as infile:
        header = infile.readline()
        part_num = 1
        current_size = 0

        # Créer le premier fichier de sortie
        output_path = os.path.join(output_dir, f"part_{part_num}.csv")
        outfile = open(output_path, 'w', encoding='utf-8')
        outfile.write(header)
        current_size += len(header.encode('utf-8'))

        for line in infile:
            line_size = len(line.encode('utf-8'))
            # Si ajouter cette ligne dépasse la taille max, on crée un nouveau fichier
            if current_size + line_size > max_size_bytes:
                outfile.close()
                part_num += 1
                output_path = os.path.join(output_dir, f"part_{part_num}.csv")
                outfile = open(output_path, 'w', encoding='utf-8')
                outfile.write(header)
                current_size = len(header.encode('utf-8'))

            # Écrire la ligne et mettre à jour la taille
            outfile.write(line)
            current_size += line_size

        outfile.close()

    print(f"Fichier '{input_file}' divisé en {part_num} parties dans le dossier '{output_dir}'.")


if __name__ == "__main__":
    # Exemple d’utilisation :
    # python split_csv_by_size.py
    input_file = "tpot_logs.csv"  # Nom du fichier d’entrée
    split_csv_by_size(input_file)
