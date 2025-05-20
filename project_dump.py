import os
from pathlib import Path


def is_hidden(path):
    """Проверяет, является ли путь скрытым (для Unix и Windows)"""
    return any(part.startswith('.') for part in Path(path).parts if part != '.')


def should_ignore(path):
    """Определяет, нужно ли игнорировать папку/файл"""
    ignore_dirs = {
        '__pycache__', '.git', '.idea', '.venv',
        'venv', 'env', 'node_modules', 'dist', 'build'
    }
    path_parts = Path(path).parts

    # Игнорировать стандартные технические папки
    if any(part in ignore_dirs for part in path_parts):
        return True

    # Игнорировать скрытые папки, кроме .env
    if is_hidden(path) and not path.endswith('.env'):
        return True

    return False


def collect_files(root_dir):
    """Рекурсивно собирает список файлов проекта"""
    project_files = []

    for root, dirs, files in os.walk(root_dir):
        # Удаляем игнорируемые папки из списка для обхода
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]

        for file in files:
            file_path = os.path.join(root, file)
            if not should_ignore(file_path):
                project_files.append(file_path)

    return sorted(project_files)


def write_project_structure(output_file, files):
    """Записывает структуру проекта в файл"""
    with open(output_file, 'w', encoding='utf-8') as f_out:
        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f_in:
                    content = f_in.read()
            except UnicodeDecodeError:
                continue  # Пропускаем бинарные файлы
            except Exception as e:
                content = f"# Error reading file: {str(e)}"

            relative_path = os.path.relpath(file_path)
            f_out.write(f"# {relative_path}\n")
            f_out.write(f"{content}\n\n")


if __name__ == "__main__":
    project_root = os.getcwd()  # Текущая директория как корень проекта
    output_filename = "project_dump.txt"

    files = collect_files(project_root)
    write_project_structure(output_filename, files)

    print(f"Project structure saved to {output_filename}")
    print(f"Total files processed: {len(files)}")