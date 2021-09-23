/*
 * Author: Alexey Mulyukin, 3121
 * Date: 21.03.2014
 */

#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <string.h>
#include <getopt.h>

#include "ipc.h"
#include "pa1.h"
#include "common.h"

pid_t parentPid;
FILE* pLogFile;
FILE* pPipeFile;
int childCount;
int processID;

typedef struct {
	int senderId;
	int receiveId;
} DataInfo;

typedef struct {
	int input;
	int output;
} PipeLineInfo;
PipeLineInfo* pPipeLines = NULL;
int pipeLinesWidth = 0;

static const char * const pipe_opened_fmt =
	"Pipe (input %5d, output %5d) has OPENED\n";

static const char * const pipe_read_fmt =
	"Pipe %5d in process %5d read %5d bytes: %s\n";

static const char * const pipe_write_fmt =
	"Pipe %5d in process %5d write %5d bytes: %s\n";

static const char * const pipe_closed_fmt =
	"Pipe %5d in process %5d has CLOSED\n";

//------------------------------------------------------------------------------

#define E_PIPE_INVALID_ARGUMENT -1
#define E_PIPE_NO_DATA -2

void initPipeLines(int processCount) {
	pipeLinesWidth = processCount - 1;
	int pipesCount = pipeLinesWidth * processCount;
	pPipeLines = (PipeLineInfo*)malloc(sizeof(PipeLineInfo) * pipesCount);

	for (int id = 0; id < pipesCount; id++) {
		int pipe_ids[2];
		pipe(pipe_ids);
		pPipeLines[id].input = pipe_ids[0];
		pPipeLines[id].output = pipe_ids[1];

		fprintf(pPipeFile, pipe_opened_fmt, pPipeLines[id].input, pPipeLines[id].output);
		fflush(pPipeFile);
	}
}

void closePipe(int fd) {
	if (fd != 0) {
		fprintf(pPipeFile, pipe_closed_fmt, fd, processID);
		fflush(pPipeFile);
		close(fd);
	}
}

int getPipeId(int from, int to) {
	if (to > from) to--;
	return from * pipeLinesWidth + to;
}

int getOpenedPipesFDCount() {
	int pipesCount = pipeLinesWidth * (pipeLinesWidth + 1);
	int result = 0;
	for (int idx = 0; idx < pipesCount; idx++) {
		if (pPipeLines[idx].input)
			result++;
		if (pPipeLines[idx].output)
			result++;
	}

	return result++;
}

void closeUnusedPipes(int selfId) {
	int pipesCount = pipeLinesWidth * (pipeLinesWidth + 1);
	for (int idx = 0; idx < pipesCount; idx++) {
		int fromProcessId = idx / pipeLinesWidth;
		int toProcessId = idx % pipeLinesWidth;
		if (toProcessId >= fromProcessId) toProcessId++;

		/*
		fprintf(pPipeFile, "self = %d; idx = %d; from = %d; to = %d; idx[c] = %d\n",
				selfId,
				idx,
				fromProcessId,
				toProcessId,
				getPipeId(fromProcessId, toProcessId)
			);
		fflush(pPipeFile);
		*/

		if (fromProcessId == selfId) {
			closePipe(pPipeLines[idx].input);
			pPipeLines[idx].input = 0;
		} else if (toProcessId == selfId) {
			closePipe(pPipeLines[idx].output);
			pPipeLines[idx].output = 0;
		} else {
			closePipe(pPipeLines[idx].input);
			pPipeLines[idx].input = 0;
			closePipe(pPipeLines[idx].output);
			pPipeLines[idx].output = 0;
		}
	}

	fprintf(pPipeFile, "Opened for %d PipesFD == %d\n", selfId, getOpenedPipesFDCount());
}

void freePipeLines() {
	if (pPipeLines != NULL) {
		int pipesCount = pipeLinesWidth * (pipeLinesWidth + 1);
		for (int id = 0; id < pipesCount; id++) {
			closePipe(pPipeLines[id].input);
			closePipe(pPipeLines[id].output);
			pPipeLines[id].input = 0;
			pPipeLines[id].output = 0;
		}

		free(pPipeLines);
		pPipeLines = NULL;
	}
}

int writePipe(int fd, const Message* msg) {
	if (fd == 0 || msg == NULL)
		return E_PIPE_INVALID_ARGUMENT;

	write(fd, &msg->s_header, sizeof(MessageHeader));
	if (msg->s_header.s_payload_len > 0) {
		write(fd, msg->s_payload, msg->s_header.s_payload_len);
	}
	fprintf(pPipeFile, pipe_write_fmt, fd, processID, sizeof(MessageHeader) + msg->s_header.s_payload_len, msg->s_payload);
	fflush(pPipeFile);

	return 0;
}

int readPipe(int fd, Message* msg, char isWait) {
	if (fd == 0 || msg == NULL)
		return E_PIPE_INVALID_ARGUMENT;

	if (!isWait) {
		int cur = lseek(fd, 0, SEEK_CUR);
		int end = lseek(fd, 0, SEEK_END);
		if (end > cur) {
			lseek(fd, cur, SEEK_SET);
			return E_PIPE_NO_DATA;
		}
	}

	read(fd, &msg->s_header, sizeof(MessageHeader));
	if (msg->s_header.s_payload_len > 0) {
		read(fd, msg->s_payload, msg->s_header.s_payload_len);
	}
	fprintf(pPipeFile, pipe_read_fmt, fd, processID, sizeof(MessageHeader) + msg->s_header.s_payload_len, msg->s_payload);
	fflush(pPipeFile);

	return 0;
}


//------------------------------------------------------------------------------

/*
 * IPC implementation
 */



/** Send a message to the process specified by id.
 *
 * @param self    Any data structure implemented by students to perform I/O
 * @param dst     ID of recepient
 * @param msg     Message to send
 *
 * @return 0 on success, any non-zero value on error
 */
int send(void * self, local_id dst, const Message * msg) {
	DataInfo* info = (DataInfo*)self;

	int pipeId = pPipeLines[getPipeId(info->senderId, dst)].output;
	return writePipe(pipeId, msg);
}

//------------------------------------------------------------------------------

/** Send multicast message.
 *
 * Send msg to all other processes including parrent.
 * Should stop on the first error.
 *
 * @param self    Any data structure implemented by students to perform I/O
 * @param msg     Message to multicast.
 *
 * @return 0 on success, any non-zero value on error
 */
int send_multicast(void * self, const Message * msg) {
	DataInfo* info = (DataInfo*)self;

	for (int id = 0; id < childCount + 1; id++) {
		if (id != info->senderId) {
			int pipeId = pPipeLines[getPipeId(info->senderId, id)].output;
			if (writePipe(pipeId, msg) < 0)
				return -1;
		}
	}

	return 0;
}

//------------------------------------------------------------------------------


/** Receive a message from the process specified by id.
 *
 * Might block depending on IPC settings.
 *
 * @param self    Any data structure implemented by students to perform I/O
 * @param from    ID of the process to receive message from
 * @param msg     Message structure allocated by the caller
 *
 * @return 0 on success, any non-zero value on error
 */
int receive(void * self, local_id from, Message * msg) {
	DataInfo* info = (DataInfo*)self;

	int pipeId = pPipeLines[getPipeId(from, info->senderId)].input;
	return readPipe(pipeId, msg, 1);
}

//------------------------------------------------------------------------------

/** Receive a message from any process.
 *
 * Receive a message from any process, in case of blocking I/O should be used
 * with extra care to avoid deadlocks.
 *
 * @param self    Any data structure implemented by students to perform I/O
 * @param msg     Message structure allocated by the caller
 *
 * @return 0 on success, any non-zero value on error
 */
int receive_any(void * self, Message * msg) {
	DataInfo* info = (DataInfo*)self;

	while (1) {
		for (int id = 0; id < childCount + 1; id++) {
			if (id != info->senderId) {
				int pipeId = pPipeLines[getPipeId(id, info->senderId)].input;

				if (readPipe(pipeId, msg, 0) == 0) {
					info->receiveId = id;

					return 0;
				}
			}
		}
	}

	return -1;
}

//------------------------------------------------------------------------------

/*
 * Child workflow
 */

int system_done(pid_t pid, int selfId) {
	// sync
	Message msg;
	sprintf(msg.s_payload, log_done_fmt, selfId);
	msg.s_header.s_magic = MESSAGE_MAGIC;
	msg.s_header.s_payload_len = strlen(msg.s_payload) + 1;
	msg.s_header.s_type = DONE;
	msg.s_header.s_local_time = time(NULL);

	DataInfo info;
	info.senderId = selfId;

	send_multicast(&info, &msg);

	for (int id = 0; id < childCount + 1; id++) {
		if (id != PARENT_ID && id != selfId) {
			do {
				receive(&info, id, &msg);
			} while (msg.s_header.s_type != DONE);
		}
	}

	// sync ended
	fprintf(pLogFile, log_received_all_done_fmt, selfId);
	fflush(pLogFile);

	return 0;
}

int system_work(pid_t pid, int selfId) {
	// some work
	int i = 10000000;
	while (i > 0) i--;

	// work is done
	fprintf(pLogFile, log_done_fmt, selfId);
	fflush(pLogFile);

	return system_done(pid, selfId);
}

int system_started(pid_t pid, int selfId) {
	// sync
	fprintf(pLogFile, log_started_fmt, selfId, pid, parentPid);
	fflush(pLogFile);

	Message msg;

	sprintf(msg.s_payload, log_started_fmt, selfId, pid, parentPid);
	msg.s_header.s_magic = MESSAGE_MAGIC;
	msg.s_header.s_payload_len = strlen(msg.s_payload) + 1;
	msg.s_header.s_type = STARTED;
	msg.s_header.s_local_time = time(NULL);

	DataInfo info;
	info.senderId = selfId;

	send_multicast(&info, &msg);

	for (int id = 0; id < childCount + 1; id++) {
		if (id != PARENT_ID && id != selfId) {
			do {
				receive(&info, id, &msg);
			} while (msg.s_header.s_type != STARTED);
		}
	}

	// sync complete
	fprintf(pLogFile, log_received_all_started_fmt, selfId);
	fflush(pLogFile);

	return system_work(pid, selfId);
}

/*
 * Main process
 */

int main(int argc, char **argv) {
	childCount = 0;

	for (int idx = 1; idx < argc; idx++) {
		char* opt = argv[idx];
		if (strcmp(opt, "-p") == 0) {
			if (argc > idx + 1) {
				char* val = argv[idx + 1];
				childCount = atoi(val);
			}
		}
	}

	if (childCount < 1) {
		return -1;
	}

	processID = 0;
	parentPid = getpid();

	pLogFile = fopen(evengs_log, "w+");
	pPipeFile = fopen(pipes_log, "w+");

	initPipeLines(childCount + 1);

	pid_t childPid;
	for (int id = 1; id < childCount + 1; id++) {
		childPid = fork();
		if (childPid >= 0) {
			if (childPid == 0) {
				// Child process
				processID = id;
				closeUnusedPipes(id);
				system_started(getpid(), id);
				break;
			} else {
				// Parent process
			}
		} else {
			// Error on fork
		}
	}
	if (childPid != 0) {
		closeUnusedPipes(PARENT_ID);

		DataInfo info;
		info.senderId = PARENT_ID;
		Message msg;

		//send_multicast(&info, &msg);

		for (int idx = 1; idx < childCount + 1; idx++) {
			receive(&info, idx, &msg);
		}

		for (int idx = 1; idx < childCount + 1; idx++) {
			receive(&info, idx, &msg);
		}

		int status;
		while (wait(&status) > 0);
	}

	freePipeLines();

	fclose(pPipeFile);
	fclose(pLogFile);

	return 0;
}
