/*

dzcp: Dragan's Zero-Copy utility for copying very large files

Copyright (C) 2003-2024 Dragan Stancevic <dragan@stancevic.com>

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the
 Free Software Foundation, Inc.
 51 Franklin Street
 Fifth Floor
 Boston, MA  02110-1301, USA.

*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/sysinfo.h>
#include <sys/wait.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>

#define MAX_RUNS 1000
#define VER "0.9"

typedef struct {
	int num_processes;
	size_t block_size;
	double elapsed_time;
	int shift_value;
} RunResult;

void about(void) {
	printf("dzcp: Dragan's Zero-Copy v%s, <dragan@stancevic.com>\n", VER);
}

void drop_caches() {
	FILE *fp = fopen("/proc/sys/vm/drop_caches", "w");
	if (fp == NULL) {
		perror("Error opening /proc/sys/vm/drop_caches");
		exit(1);
	}
	fprintf(fp, "3");
	fclose(fp);
}

void copy_blocks(const char *source_file, const char *dest_file, off_t file_size, int process_num, int num_processes, size_t block_size) {
	/* initial offset for this process */
	off_t offset = process_num * block_size;
	off_t dest_offset = offset;
	ssize_t to_send, bytes_sent;
	int dest_fd, source_fd;

	/* each process opens its own source and destination file descriptors */
	source_fd = open(source_file, O_RDONLY);
	if (source_fd < 0) {
		perror("Error opening source file in child process");
		exit(1);
	}

	dest_fd = open(dest_file, O_WRONLY);
	if (dest_fd < 0) {
		perror("Error opening destination file in child process");
		close(source_fd);
		exit(1);
	}

	/* print offsets being written */
	// printf("process %d: writing from offset %lld to offset %lld\n", process_num, (long long)offset, (long long)dest_offset);

	while (offset < file_size) {
		// Ensure the destination file is correctly positioned
		if (lseek(dest_fd, dest_offset, SEEK_SET) == (off_t)-1) {
			perror("Error seeking in destination file");
			close(source_fd);
			close(dest_fd);
			exit(1);
		}

		/* perform the sendfile operation */
		to_send = block_size;
		while (to_send > 0 && offset < file_size) {
			bytes_sent = sendfile(dest_fd, source_fd, &offset, to_send);
			if (bytes_sent <= 0) {
				if (errno == EINTR || errno == EAGAIN) {
					/* retry in case of interruptions or non-blocking operation */
					continue;
				} else {
					perror("Error during sendfile");
					close(source_fd);
					close(dest_fd);
					exit(1);
				}
			}
			to_send -= bytes_sent;
		}

		/* move the destination and source file pointers for the next block handled by this process */
		/* skip blocks for the other processes */
		dest_offset += num_processes * block_size;
		/* skip source blocks that other processes will handle */
		offset += (num_processes - 1) * block_size;
	}

	/* close file descriptors after done */
	close(source_fd);
	close(dest_fd);
}

void perform_copy(int num_processes, size_t block_size, const char *source_file, const char *dest_file, RunResult *result) {
	struct stat file_stat;
	off_t file_size;
	pid_t pid;
	int dest_fd;

	if (stat(source_file, &file_stat) < 0) {
		perror("Error getting file status");
		exit(1);
	}
	file_size = file_stat.st_size;

	/* parent process: ensure the destination file is created if it doesn't exist */
	dest_fd = open(dest_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (dest_fd < 0) {
		perror("Error creating destination file");
		exit(1);
	}
	/* parent closes the file; child processes will reopen it */
	close(dest_fd);

	struct timeval start_time, end_time;
	gettimeofday(&start_time, NULL);

	/* fork processes to zero-copy the file in parallel */
	for (int i = 0; i < num_processes; i++) {
		pid = fork();
		if (pid < 0) {
			perror("Error forking process");
			exit(1);
		} else if (pid == 0) {
			/* child process perform the file copy with its own file descriptors */
			copy_blocks(source_file, dest_file, file_size, i, num_processes, block_size);
			/* exit the child process */
			exit(0);
		}
	}

	/* parent process waits for all child processes */
	for (int i = 0; i < num_processes; i++) {
		wait(NULL);
	}

	gettimeofday(&end_time, NULL);
	result->num_processes = num_processes;
	result->block_size = block_size;
	result->elapsed_time = (end_time.tv_sec - start_time.tv_sec) + 
						   (end_time.tv_usec - start_time.tv_usec) / 1000000.0;

	double throughput = (double)file_size / (1024.0 * 1024.0 * result->elapsed_time);
	printf("Operation completed in %.2f seconds.\n", result->elapsed_time);
	printf("Throughput: %.2f MiB/s\n", throughput);
}

int compare_run_results(const void *a, const void *b) {
	const RunResult *run_a = (const RunResult *)a;
	const RunResult *run_b = (const RunResult *)b;
	return (run_a->elapsed_time > run_b->elapsed_time) - (run_a->elapsed_time < run_b->elapsed_time);
}

void find_optimal_settings(const char *source_file, const char *dest_file) {
	int processes_per_cpu, num_processes, num_cpus = get_nprocs();
	/* 64KiB to 1024KiB */
	size_t block_sizes[] = {64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024};
	// RunResult results[MAX_RUNS] = {{0, }, };
	RunResult *results;
	int i, shift_value, run_index = 0;

	results = malloc(MAX_RUNS * sizeof(RunResult));
	if (results == NULL) {
		perror("Failed to allocate memory for results");
		exit(1);
	}
	memset(results, 0, MAX_RUNS * sizeof(RunResult));

	for (processes_per_cpu = 1; processes_per_cpu <= 6; processes_per_cpu++) {
		num_processes = processes_per_cpu * num_cpus;
		for (i = 0; i < sizeof(block_sizes) / sizeof(block_sizes[0]); i++) {
			if (run_index >= MAX_RUNS) {
				fprintf(stderr, "Exceeded maximum runs.\n");
				break;
			}

			/* drop caches to flush page cache */
			drop_caches();

			/* shift starts at 6 and goes to 10 */
			shift_value = 6 + i;
			printf("Testing with -p %d and -s %d (%zu KiB)\n", num_processes, shift_value, block_sizes[i] / 1024);
			results[run_index].shift_value = shift_value;
			perform_copy(num_processes, block_sizes[i], source_file, dest_file, &results[run_index]);

			/* remove destination file for next run */
			if (unlink(dest_file) < 0) {
				perror("Error deleting destination file");
				free(results);
				exit(1);
			}

			run_index++;
		}
	}

	/* sort the results based on elapsed_time (ascending) */
	qsort(results, run_index, sizeof(RunResult), compare_run_results);

	printf("\nFastest 5 runs:\n");
	for (i = 0; i < 5 && i < run_index; i++) {
		printf("Run %d: -p %d -s %d (%zu KiB), %.2f seconds\n", i + 1,
			   results[i].num_processes, results[i].shift_value, results[i].block_size / 1024, results[i].elapsed_time);
	}

	printf("\nSlowest 5 runs:\n");
	for (i = run_index - 1; i >= run_index - 5 && i >= 0; i--) {
		printf("Run %d: -p %d -s %d (%zu KiB), %.2f seconds\n", run_index - i,
			   results[i].num_processes, results[i].shift_value, results[i].block_size / 1024, results[i].elapsed_time);
	}

	free(results);
}

int main(int argc, char *argv[]) {
	int opt;
	int num_processes = 0;
	int shift_value = 0;
	int optimize = 0;
	size_t block_size;

	about();

	/* parse command line arguments */
	while ((opt = getopt(argc, argv, "p:s:o")) != -1) {
		switch (opt) {
			case 'p':
				num_processes = atoi(optarg);
				break;
			case 's':
				/* calculate block size based on shift */
				shift_value = atoi(optarg);
				block_size = 64 * 1024 * (1 << (shift_value - 6));
				break;
			case 'o':
				optimize = 1;
				if (geteuid() != 0) {
					fprintf(stderr, "You need to be root to run with -o option.\n");
					exit(1);
				}
				break;
			default:
				fprintf(stderr, "Usage: %s [-p num_processes] [-s shift_value] [-o] <source> <destination>\n", argv[0]);
				return 1;
		}
	}

	if (optind + 2 > argc) {
		fprintf(stderr, "Usage: %s [-p num_processes] [-s shift_value] [-o] <source> <destination>\n", argv[0]);
		return 1;
	}

	if (num_processes == 0) {
		int num_cpus = get_nprocs();
		num_processes = num_cpus * 4;
	}

	/* verify that shift value is set */
	if (shift_value == 0) {
		shift_value = 10;
		block_size = 64 * 1024 * (1 << (shift_value - 6));
	}

	if (optimize) {
		find_optimal_settings(argv[optind], argv[optind + 1]);
	} else {
		RunResult result;
		printf("Starting %d processes with a transfer size of %zu KiB per block.\n", num_processes, block_size / 1024);
		perform_copy(num_processes, block_size, argv[optind], argv[optind + 1], &result);
	}

	return 0;
}
