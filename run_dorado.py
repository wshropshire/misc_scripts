#!/usr/bin/env python

import argparse
import os
import glob
import subprocess

# Usage: ./run_dorado.py /path/to/pod5/files --threads 16 --sample_sheet sample_sheet.csv --kit_name SQK-RBK114-96 --min_qscore 10 --output_dir my_output --prefix my_sample

# Reminder: Make sure sample sheet adheres to guidelines as established by ONT to ensure will run properly. See example in directory for ONT sample sheet files

# Currently Dorado and model file are hardcoded. Seadragon won't allow for downloading models due to firewall constraints, so have to provide absolute pathway for model. Will need to modify if decide to have option for methylation models

def basecaller(pod5_path, threads, kit_name, min_qscore, output_dir, prefix):

    calls_path = os.path.join(output_dir, f'{prefix}_calls.bam')

    basecaller_cmd = [
        '/rsrch3/home/infec_dis/Shelburne_Lab/programs/dorado/dorado-0.5.1-linux-x64/bin/dorado',
        'basecaller',
        '/rsrch3/home/infec_dis/Shelburne_Lab/programs/dorado/dorado-0.5.1-linux-x64/data/dna_r10.4.1_e8.2_400bps_sup@v4.3.0',
        pod5_path,
        '-v',
        '-x', 'cuda:all',
        '--kit-name', kit_name,
        '--min-qscore', str(min_qscore)
    ]

    # Redirecting output to the specified calls_path.bam file
    with open(calls_path, 'w') as output_file:
        subprocess.run(basecaller_cmd, check=True, stdout=output_file)

    return calls_path

def demux(threads, sample_sheet, kit_name, output_dir, prefix):
    
    # Create new output directory for demux fastq files
    output_dir2 = os.path.join(output_dir, f'{prefix}_demux_fastq_files')
    calls_path = os.path.join(output_dir, f'{prefix}_calls.bam')
    demux_cmd = [
        '/rsrch3/home/infec_dis/Shelburne_Lab/programs/dorado/dorado-0.5.1-linux-x64/bin/dorado',
        'demux',
        calls_path,
        '--emit-fastq',
        '--output-dir', output_dir2,
        '--kit-name', kit_name,
        '--threads', str(threads),
        '-v',
        '--sample-sheet', sample_sheet
    ]

    subprocess.run(demux_cmd, check=True, stdout=subprocess.PIPE)

def post_process(output_dir, threads, nanoplot_path, prefix):

    output_dir2 = os.path.join(output_dir, f'{prefix}_demux_fastq_files')
    os.chdir(output_dir2)
    # Remove unclassified fastq files
    os.remove('unclassified.fastq')
    # Remove fastq files that are less than 10M
    fastq_files = glob.glob('*fastq')
    for file in fastq_files:
        file_size = os.path.getsize(file)
        if file_size < 10 * 1024 * 1024:  # Convert size to bytes
            os.remove(file)

    # Gzip fastq files
    fastq_files2 = glob.glob('*fastq')
    subprocess.run(['gzip'] + fastq_files2, check=True)
    
    # Run NanoPlot on fastq files
    for f in os.listdir('.'):
        if f.endswith('.fastq.gz'):
            prefix2 = os.path.splitext(os.path.splitext(f)[0])[0]
            nano_cmd = [
                nanoplot_path,
                '--threads', str(threads),
                '--verbose',
                '-p', f'{prefix2}_',
                '-o', f'{prefix2}_NanoPlot',
                '--info_in_report',
                '--only-report',
                '--N50', 
                '--no_static',
                '--fastq', f
            ]
            subprocess.run(nano_cmd, check=True, stdout=subprocess.PIPE)

def main():
    parser = argparse.ArgumentParser(description='Dorado Basecalling, Demux, and QC Script')
    parser.add_argument('pod5_path', help='Path to pod5 files')
    parser.add_argument('--threads', type=int, default=8, help='Number of threads (default: 8)')
    parser.add_argument('--sample_sheet', default='', help='Sample sheet file (default: "")')
    parser.add_argument('--kit_name', default='SQK-RBK114-96', help='Kit name (default: SQK-RBK114-96)')
    parser.add_argument('--min_qscore', type=int, default=8, help='Minimum quality score (default: 8)')
    parser.add_argument('--output_dir', default=os.getcwd(), help='Output directory (default: current working directory)')
    parser.add_argument('--prefix', default='sample', help='Prefix for output files (default: sample)')
    parser.add_argument('--nanoplot_path', default='NanoPlot', help='Path to NanoPlot executable (default: NanoPlot)')

    args = parser.parse_args()

    basecaller(args.pod5_path, args.threads, args.kit_name, args.min_qscore, args.output_dir, args.prefix)
    demux(args.threads, args.sample_sheet, args.kit_name, args.output_dir, args.prefix)
    post_process(args.output_dir, args.threads, args.nanoplot_path, args.prefix)

if __name__ == "__main__":
    main()

