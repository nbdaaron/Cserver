#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> //readdir
#include <dirent.h> //readdir 
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <netdb.h>
#include "sorter_client.h"

int main(int argc, char **argv) 
{
	if (argc<7) {
		printf("Missing arguments\n");
		printf("Usage: ./sorter_client -c <column> -h <hostname or ip address> -p <port number> [-d <dirname>] [-o <output dirname>]\n");
		exit(0);
	} else if (argc % 2 != 1) {
		printf("Usage: ./sorter_client -c <column> -h <hostname or ip address> -p <port number> [-d <dirname>] [-o <output dirname>]\n");
		exit(0);
	}
	
	char *column = NULL;
	char *hostname = NULL;
	char *portNumber = NULL;
	char *directoryName = NULL;
	char *outputDirectoryName = NULL;
	
	int i;
	for (i=1;i<argc;i+=2)
	{
		if (!strcmp(argv[i],"-c")) 
		{
			column = argv[i+1];
		}
		else if (!strcmp(argv[i],"-h")) 
		{
			hostname = argv[i+1];
		}
		else if (!strcmp(argv[i],"-p")) 
		{
			portNumber = argv[i+1];
		}
		else if (!strcmp(argv[i],"-d")) 
		{
			directoryName = argv[i+1];
		}
		else if (!strcmp(argv[i],"-o")) 
		{
			outputDirectoryName = argv[i+1];
			DIR *dir = opendir(outputDirectoryName);
			if (!dir && ENOENT == errno) 
			{
				printf("ERROR: Cannot open output directory: %s.\n", outputDirectoryName);
				exit(0);
			}
		} 
		else 
		{
			printf("Unknown Flag %s\n", argv[i]);
			return 0;
		}
	}
	
	if (column == NULL || hostname == NULL || portNumber == NULL) {
		printf("Missing required flags, (-c -h -p)\n");
		exit(0	);
	}
	
	printf("column=%s, hostname=%s, portNumber=%s, directoryName=%s, outputDirectoryName=%s\n", column, hostname, portNumber, directoryName, outputDirectoryName);
	
	sem_init(&openedFiles, 0, maxOpenedFileLimit);
	
	// send all files to be sorted
	parseAndSendDir(hostname, portNumber, directoryName, column);
	
	// get dump
	int sockfd = createSocket(hostname, portNumber);
	sendRequest(sockfd, getDump, column, NULL);
	
	struct csv *csv = readDump(sockfd);
	
	// determine output location
	char *outputLocation;
	if (outputDirectoryName != NULL) {
		outputLocation = calloc(1, (strlen("/AllFiles-Sorted-.csv") + strlen(outputDirectoryName) + strlen(column) + 2) * sizeof(char));
		strcat(outputLocation, outputDirectoryName);
		strcat(outputLocation, "/AllFiles-Sorted-");
		strcat(outputLocation, column);
		strcat(outputLocation, ".csv");
	} else {
 		outputLocation = calloc(1, (strlen("AllFiles-Sorted-.csv") + strlen(column) + 2) * sizeof(char));
		strcat(outputLocation, "AllFiles-Sorted-");
		strcat(outputLocation, column);
		strcat(outputLocation, ".csv");
	}
	
	FILE *out = fopen(outputLocation, "w");
	printCSV(csv, out);
	freeCSV(csv);
	return 0;
}


int parseAndSendDir(char *host, char *portNumber, char *inputDir, char *sortBy)
{
	struct dirent * pDirent;
	DIR *dir = NULL;
	
	if (inputDir == NULL) 
	{
		inputDir = ".";
	}
	dir = opendir(inputDir);
	if (dir == NULL) 
	{
		printf("ERROR: Cannot open input directory: %s.\n", inputDir);
		exit(0);
	}
	
	int maxPossibleThreads = 5000;
	unsigned long *listOfThreadIDs = (unsigned long *) malloc(maxPossibleThreads*sizeof(unsigned long));
	int numChildThreads = 0;
	
	int totalNumThreads = 1;
	
	while (((pDirent = readdir(dir)) != NULL)) 
	{
		//files
		if (isCSV(pDirent->d_name) && pDirent->d_type == DT_REG) 
		{
			printf("%s\n", pDirent->d_name);
			pthread_t tid;
			struct sendFileArguments *sortFileParameters = (struct sendFileArguments *) malloc(sizeof(struct sendFileArguments));
			sortFileParameters->host = host;
			sortFileParameters->portNumber = portNumber;
			sortFileParameters->inputDir = inputDir;
			sortFileParameters->fileName = (char *) calloc(1, (int)strlen(pDirent->d_name)*sizeof(char)+2);
			strcat(sortFileParameters->fileName, pDirent->d_name);
			sortFileParameters->sortBy = sortBy;
			
			pthread_create(&tid, NULL, threadSendFile, (void *)sortFileParameters);
			
			if (numChildThreads<maxPossibleThreads)
			{
				listOfThreadIDs[numChildThreads] = tid;
				numChildThreads++;
			} else {
				maxPossibleThreads = maxPossibleThreads*2;
				unsigned long *tempPtr= (unsigned long *)realloc(listOfThreadIDs, maxPossibleThreads * sizeof(unsigned long));
				listOfThreadIDs = tempPtr;
				listOfThreadIDs[numChildThreads] = tid;
				numChildThreads++;
			}
		} //directories
		else if (pDirent->d_type == DT_DIR && (strcmp(pDirent->d_name, ".")) && (strcmp(pDirent->d_name, "..")) && (strcmp(pDirent->d_name, ".git"))) 
		{
			// replace this with recursive code to avoid threading
			//printf("DIRECTORY: %s in %s\n", pDirent->d_name, inputDir);
			
			char *subDir = (char *)calloc(1, (strlen(inputDir)+strlen(pDirent->d_name)+2));
			strcat(subDir, inputDir);
			strcat(subDir, "/");
			strcat(subDir, pDirent->d_name);
			
			parseAndSendDir(host, portNumber, subDir, sortBy);
			free(subDir);
			
		}
	}
	closedir(dir);
	
	int i;
	int status = 0;
	
	// printf("I'm a thread=%lu\n ", pthread_self());
	for (i=0;i<numChildThreads;i++) 
	{
		printf("Join here %d\n", i);
		pthread_join(listOfThreadIDs[i], (void *)&status);  //blocks execution until thread is joined
		//printf("Join %d number=%lu\t with retval=%d\n", i, (unsigned long)listOfThreadIDs[i], (int)status);
		totalNumThreads += status;
	}
	free(listOfThreadIDs);
	return totalNumThreads;
}

void *threadSendFile(void *args)
{
	sem_wait(&openedFiles);
	
	struct sendFileArguments *arguments = (struct sendFileArguments *) args;
	
	int sockfd = createSocket(arguments->host, arguments->portNumber);
	
	FILE *in;
	if (arguments->inputDir != NULL) 
	{
		char *inputLocation = calloc(1, (strlen(arguments->inputDir) + strlen(arguments->fileName) + 2) * sizeof(char)); //this line is breaking
		strcat(inputLocation, arguments->inputDir);
		strcat(inputLocation, "/");
		strcat(inputLocation, arguments->fileName);
		in = fopen(inputLocation, "r");
		free(inputLocation);
	} 
	else 
	{
		in = fopen(arguments->fileName, "r");
	}
	struct csv *csv = parseCSV(in);
	
	// (int sockfd, enum requestType type, char *sortBy, struct csv *csv)
	sendRequest(sockfd, sort, arguments->sortBy, csv);
	// read from sockfd to make sure the server acknowledges my request
	printf("Waiting for Acknowldgement.\n");
	readAcknowledgement(sockfd);
	
	printf("Sent Request to Server.\n");
	
	close(sockfd);
	
	free(arguments);
	int retval = 1;
	
	sem_post(&openedFiles);
	
	pthread_exit((void *) (intptr_t) retval);
	return NULL;
}

int createSocket(char *hostname, char *portNumber) {
	
	int success;
	
	struct addrinfo *addresses;
	
	struct addrinfo hints;
	bzero(&hints, sizeof(struct addrinfo));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	
	//Set up local socket // socket creation
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	
	if (sockfd < 0) {
		printf("Error creating socket!\n");
		exit(0);
	}
	
	//Set up server address info.
	// convert domain name, hostname, and IP addresses into addresses
	// argv[1] = hostname = "www.example.com", an ip ; argv[2] = service = port number, "echo"
	success = getaddrinfo(hostname, portNumber, &hints, &addresses);
	
	if (success < 0) {
		printf("Error getting address info!\n");
		exit(0);
	}
	
	//Attempt to connect local socket to server.
	// socket file descriptor, sockaddr ai_addr (info), size of sockaddr 
	success = connect(sockfd, (struct sockaddr *)addresses[0].ai_addr, sizeof(struct sockaddr));
	
	if (success < 0) {
		printf("Error connecting to server!\n");
		exit(0);
	}
	
	return sockfd;
}

///Parses CSV and returns a pointer to CSV.
struct csv *parseCSV(FILE *file) 
{
	//Pointer to CSV file that will be returned.
	struct csv *ret = malloc(sizeof(struct csv));
	
	//Retrieve header info and populate CSV with values.
	struct headerInfo headerInfo = getHeaderInfo(file);
	ret->columnTypes = headerInfo.types;
	ret->columnNames = headerInfo.columnNames;
	//populate entries and total number of entries
	struct entryInfo entryInfo = getCSVEntries(file, ret->columnTypes);
	ret->entries = entryInfo.entries;
	ret->numEntries = entryInfo.numEntries;
	
	return ret;
}

///Parse first line of CSV and get array of data types for values.
struct headerInfo getHeaderInfo(FILE *file) 
{
	int localMaxStringSize = maxStringSize;

	struct headerInfo ret;

	char **columnNames = malloc(sizeof(char *) * columns);
	enum type *types = malloc(sizeof(enum type) * columns);

	char *currentInput;
	char nextChar;
	int stringPosition, retPosition=0;
	char newlineFound = 0;

	//Ignore leading comma if exists.
	fscanf(file, ",");
	
	while (!newlineFound) 
	{
		currentInput = malloc(sizeof(char) * maxStringSize);
		stringPosition = 0;
		while (fscanf(file, "%c", &nextChar) > 0) 
		{
			if (nextChar == ',') 
			{
				break;
			} 
			else if (nextChar == '\n' || nextChar == '\r') 
			{
				fscanf(file, "\r");
				fscanf(file, "\n");
				newlineFound = 1;
				break;
			}
			currentInput = addCharacterToString(currentInput, nextChar, stringPosition++, &localMaxStringSize);
		}
		//Add null-terminating 0 to end of String.
		currentInput = addCharacterToString(currentInput, '\0', stringPosition, &localMaxStringSize);
		types[retPosition] = getTypeFromColumnName(currentInput);
		columnNames[retPosition] = currentInput;

		retPosition++;
	}
	if (retPosition != 28) 
	{
		exit(0);
	}

	ret.columnNames = columnNames;
	ret.types = types;

	return ret;
}

///Retrieves CSV Entries through fscanf and returns array of entries as well as entry count.
struct entryInfo getCSVEntries(FILE *file, enum type *columnTypes) 
{

	int localMaxStringSize = maxStringSize;

	//Return value: Array of Entry Pointers.
	struct entry **ret = malloc(sizeof(struct entry *) * maxEntries);

	char eofReached = 0, newlineFound = 0, next;
	int scanResult;

	char *currentString = malloc(sizeof(char)*maxStringSize);;
	int stringPosition;

	int quotationMarksFound = 0;
	int nonWhiteSpaceFound = 0;

	int currentEntryPosition = 0;
	int currentValuePosition = 0;
	struct entry *currentEntry;

	//Loop through each line until end of file reached.
	while (!eofReached) 
	{
		newlineFound = 0, stringPosition = 0, quotationMarksFound = 0, nonWhiteSpaceFound = 0;

		//For each line, a new entry will be created (with an array of value pointers).
		currentEntry = malloc(sizeof(struct entry));
		currentEntry -> values = malloc(sizeof(union value) * columns);

		//Loop through each character within a line until line break or end of file reached.
		while (!newlineFound && !eofReached) 
		{
			scanResult = fscanf(file, "%c", &next);
			if (scanResult == EOF) 
			{
				eofReached = 1;
				if (stringPosition == 0) 
				{
					break;
				}
				currentString = addCharacterToString(currentString, '\0', stringPosition, &localMaxStringSize);

				setValue(&(currentEntry -> values[currentValuePosition]), currentString, columnTypes[currentValuePosition]);
				currentValuePosition++;

				stringPosition = 0;

				currentString = malloc(sizeof(char)*localMaxStringSize);
				break;
			}
			if (next == '\r' || next == '\n') 
			{
				fscanf(file, "\r");
				fscanf(file, "\n");
				if (stringPosition == 0) 
				{
					continue;
				}
				currentString = addCharacterToString(currentString, '\0', stringPosition, &localMaxStringSize);
				setValue(&(currentEntry -> values[currentValuePosition]), currentString, columnTypes[currentValuePosition]);
				currentValuePosition++;

				stringPosition = 0;


				currentString = malloc(sizeof(char)*localMaxStringSize);
				newlineFound = 1;
				break;
			} 
			else if (next == '"') 
			{
				//If quotation marks found, ignore any commas until next quotation mark found.
				quotationMarksFound = !quotationMarksFound;
			} 
			else if (next == ',' && !quotationMarksFound) 
			{
				currentString = addCharacterToString(currentString, '\0', stringPosition, &localMaxStringSize);
				setValue(&(currentEntry -> values[currentValuePosition]), currentString, columnTypes[currentValuePosition]);
				currentValuePosition++;

				//For now, if the CSV file has too many columns in a single row, ignore the rest of that row.
				//Should be fixed when quotation marks are handled.
				if (currentValuePosition > columns) 
				{
					newlineFound = 1;
				}
				stringPosition = 0;	

				currentString = malloc(sizeof(char)*localMaxStringSize);
			} 
			else 
			{
				if (nonWhiteSpaceFound || (next != ' ' && next != '\t')) 
				{
					currentString = addCharacterToString(currentString, next, stringPosition++, &localMaxStringSize);
					nonWhiteSpaceFound = 1;
				}
			}
		}

		ret = addEntryToArray(ret, currentEntry, currentEntryPosition++);

		currentEntry = malloc(sizeof(struct entry));
		currentEntry -> values = malloc(sizeof(union value) * columns);
		currentValuePosition = 0;
		//printf("Current Entry: %d\n", currentEntryPosition);
	}

	struct entryInfo ei;
	ei.entries = ret;
	ei.numEntries = currentEntryPosition - 1;

	return ei;
}

enum type getTypeFromColumnName(char *name) 
{
	int i;
	for (i=0;i<stringValuesSize;i++) 
	{
		if (strcmp(stringValues[i], name) == 0) 
		{
			return string;
		}
	}

	for (i=0;i<intValuesSize;i++) 
	{
		if (strcmp(intValues[i], name) == 0) 
		{
			return integer;
		}
	}

	for (i=0;i<doubleValuesSize;i++) 
	{
		if (strcmp(doubleValues[i], name) == 0) 
		{
			return decimal;
		}
	}
	return error;
}

void printCSV(struct csv *csv, FILE *file) 
{
	struct entry** entries = csv->entries;
	long size = csv->numEntries;
	int i;
	int j;

	for (i=0;i<columns;i++) 
	{
		if (i>0) 
		{
			fprintf(file, ",");
		}
		fprintf(file, "%s", csv->columnNames[i]);
	}
	fprintf(file, "\n");

	for (i=0; i<size; i++)
	{
		for (j=0; j<columns; j++) 
		{
			if (j>0) 
			{
				fprintf(file, ",");
			}
			enum type columnType = csv->columnTypes[j];
			if (columnType == string) 
			{
				fprintf(file, "\"%s\"", entries[i]->values[j].stringVal);
			} 
			else if (columnType == integer) 
			{
				fprintf(file, "%ld", entries[i]->values[j].intVal);
			} 
			else if (columnType == decimal) 
			{
				fprintf(file, "%f", entries[i]->values[j].decimalVal);
			}
		}
		fprintf(file, "\n");
	}
	return;
}

///Frees CSV struct pointer for future usage.
void freeCSV(struct csv *csv) 
{
	int i;
	//Free Column Types (Array of Enums)
	free(csv->columnTypes);

	//Free Each Individual Column Name (Dynamically Allocated String)
	for (i=0;i<columns;i++) 
	{
		free(csv->columnNames[i]);
	}

	//Free Column Names (Array of Dynamically Allocated Strings)
	free(csv->columnNames);
	//Free Each Individual CSV Entry (Array of Value Structs)
	for (i=0;i<csv->numEntries;i++) 
	{
		free(csv->entries[i]);
	}

	//Free CSV Entries (Array of Entry Pointers)
	free(csv->entries);

	//Free CSV (Dynamically Allocated Struct)
	free(csv);

}

char *addCharacterToString(char *string, char next, int position, int *localMaxStringSize) 
{
	if (position >= *localMaxStringSize) 
	{
		*localMaxStringSize *= 10;
		string = realloc(string, sizeof(char) * *localMaxStringSize);
	}
	string[position] = next;

	return string;
}

struct entry **addEntryToArray(struct entry **array, struct entry *entry, int position) 
{
	if (position >= maxEntries) 
	{
		maxEntries *= 10;
		array = realloc(array, sizeof(struct entry) * maxEntries);
	}
	array[position] = entry;

	return array;
}

void setValue(union value *location, char *value, enum type dataType) 
{
	if (dataType == string) 
	{
		location->stringVal = value;
	} 
	else if (dataType == integer) 
	{
		location->intVal = atoi(value);
	} 
	else if (dataType == decimal)
	{
		location->decimalVal = atof(value);
	} 
	else 
	{
		printf("Error: Unknown column type for value: %s.\n", value);
	}
}

int isCSV(char *fname)
{
	if (strlen(fname)<5) 
	{
		return 0;
	}
	int i = 0;
	while (fname[i]!='\0') 
	{
		i++;
	}
	if (fname[i-4]=='.' && fname[i-3]=='c' && fname[i-2]=='s' && fname[i-1]=='v') 
	{
		return 1;
	}
	return 0;
}

//CSV Must be freed by caller.
struct csv *readDump(int sockfd) {

	usleep(10000);

	struct csv *ret = malloc(sizeof(struct csv));
	ret->columnNames = malloc(sizeof(char *) * columns);
	ret->columnTypes = malloc(sizeof(enum type) * columns);

	int success, i, j;

	//printf("Reading Dump Result!!!\n");

	//Read in column names.
	for (i=0;i<columns;i++) {

		size_t stringLength = 0;
		success = read(sockfd, &stringLength, sizeof(size_t));

		if (success < 0) {
			printf("Error Reading in String Length value: %s\n", strerror(errno));
			exit(0);
		}

		char *stringValue = malloc(sizeof(char) * (stringLength + 1));
		success = read(sockfd, stringValue, stringLength);
		stringValue[stringLength] = '\0';
		ret->columnNames[i] = stringValue;

		if (success < 0) {
			printf("Error Reading Column Name!\n");
			exit(0);
		}
	}

	//Read in column types.
	for (i=0;i<columns;i++) {
		char columnType[2];
		bzero(columnType, 2);
		success = read(sockfd, columnType, 1);
		if (success < 0) {
			printf("Error Reading Column Type!\n");
			exit(0);
		}
		//printf("Column Type %d: %c\n", i, columnType[0]);
		if (columnType[0] == 'S') {
			ret->columnTypes[i] = string;
		} else if (columnType[0] == 'I'){
			ret->columnTypes[i] = integer;
		} else if (columnType[0] == 'D') {
			ret->columnTypes[i] = decimal;
		} else {
			printf("Invalid Column Type Found: %c\n", columnType[0]);
			exit(0);
		}
	}

	//Read in number of entries.
	success = read(sockfd, &(ret->numEntries), sizeof(int));
	if (success < 0) {
		printf("Error Reading Number of Entries!\n");
		exit(0);
	}
	
	//Create that many entry pointers.
	ret->entries = malloc(sizeof(struct entry *) * ret->numEntries);
	//printf("Entries: %d\n", ret->numEntries);

	//Read in all csv values.
	for (i=0;i<ret->numEntries;i++) {
		//printf("ROW %d!\n", i);
		ret->entries[i] = malloc(sizeof(struct entry));
		ret->entries[i]->values = malloc(sizeof(union value) * columns);
		for (j=0;j<columns;j++) {
			//printf("COLUMN %d!\n", j);
			if (ret->columnTypes[j] == integer) {
				success = read(sockfd, &(ret->entries[i]->values[j].intVal), sizeof(long));
				//printf("Entry: %d\n", ret->entries[i]->values[j].intVal);
			} else if (ret->columnTypes[j] == decimal) {
				success = read(sockfd, &(ret->entries[i]->values[j].decimalVal), sizeof(double));
				//printf("Entry: %d\n", ret->entries[i]->values[j].decimalVal);
			} else if (ret->columnTypes[j] == string) {
				size_t stringLength = 0;
				success = read(sockfd, &stringLength, sizeof(size_t));
				//printf("%ld ", stringLength);
				fflush(stdout);

				if (success < sizeof(size_t)) {
					printf("Error (Ret %d): Reading in String Length value: %s\n", success, strerror(errno));
					exit(0);
				}

				char *stringValue = malloc(sizeof(char) * (stringLength + 1));
				success = read(sockfd, stringValue, stringLength);
				stringValue[stringLength] = '\0';
				ret->entries[i]->values[j].stringVal = stringValue;
				//printf("Entry: %s\n", ret->entries[i]->values[j].stringVal);
			} else {
				printf("Unknown data type found in column %d!\n", j);
				exit(0);
			}

			if (success < 0) {
				printf("Error Reading in a value: %s\n", strerror(errno));
				exit(0);
			}
		}
	}

	return ret;

}

void sendCSV(int sockfd, struct csv *csv) {
	int i, j, success;
	char columnName[50];
	size_t length;
	bzero(columnName, 50);

	//Print Columns (Write 50 characters at once).
	for (i=0 ; i < columns ; i++) {
		length = strlen(csv->columnNames[i]);
		success = write(sockfd, &length, sizeof(size_t));
		success = write(sockfd, csv->columnNames[i], length);


		if (success <= 0) {
			printf("Writing column names failed.\n");
			exit(0);
		}
	}

	//Print Column Types ('S', 'I', or 'D')
	for (i=0;i < columns ; i++) {
		if (csv->columnTypes[i] == string) {
			success = write(sockfd, "S", 1);
			//printf("Writing Column Type: S\n");
		} else if (csv->columnTypes[i] == integer) {
			success = write(sockfd, "I", 1);
			//printf("Writing Column Type: I\n");
		} else if (csv->columnTypes[i] == decimal) {
			success = write(sockfd, "D", 1);
			//printf("Writing Column Type: D\n");
		} else {
			printf("Invalid Column Type Found: %d\n", csv->columnTypes[i]);
		}

		if (success <= 0) {
			printf("Writing column types failed: %s\n", strerror(errno));
			exit(0);
		}
	}

	//Print number of entries in CSV.
	write(sockfd, &(csv->numEntries), sizeof(int));
	for (i=0;i<csv->numEntries;i++) {
		for (j=0;j<columns;j++) {
			if (csv->columnTypes[j] == string) {
				length = strlen(csv->entries[i]->values[j].stringVal);
				success = write(sockfd, &length, sizeof(size_t));
				//printf("%ld ", length);
				success = write(sockfd, csv->entries[i]->values[j].stringVal, strlen(csv->entries[i]->values[j].stringVal));
			} else if (csv->columnTypes[j] == integer) {
				success = write(sockfd, &(csv->entries[i]->values[j].intVal), sizeof(long));
			} else if (csv->columnTypes[j] == decimal) {
				success = write(sockfd, &(csv->entries[i]->values[j].decimalVal), sizeof(double));
			} else {
				printf("Invalid Column Type Found: %d\n", csv->columnTypes[i]);
			}

			if (success <= 0 &&  (csv->columnTypes[j] != string || strlen(csv->entries[i]->values[j].stringVal) > 0)) {
				printf("Writing CSV Entry Row %d Col %d failed: %s\n", i, j, strerror(errno));
				exit(0);
			}

		}
	}
	
}

//If requesting dump, csv should be NULL.
void sendRequest(int sockfd, enum requestType type, char *sortBy, struct csv *csv) {
	int success;

	//Send Request Type.
	if (type == sort) {
		success = write(sockfd, "S", 1);
	} else if (type == getDump) {
		success = write(sockfd, "D", 1);
	} else {
		printf("Invalid Request Type: %d\n", type);
		exit(0);
	}

	if (success <= 0) {
		printf("Writing request type failed.\n");
		exit(0);
	}

	//Send Column to sort by.
	char sortByScaledTo50[50];
	bzero(sortByScaledTo50, 50);
	strcpy(sortByScaledTo50, sortBy);
	success = write(sockfd, sortByScaledTo50, 50);

	if (success <= 0) {
		printf("Writing column to sort by failed.\n");
		exit(0);
	}
	if (type == sort) {
		sendCSV(sockfd, csv);
	}
}

void readAcknowledgement(int sockfd) {
	char acknowlegment[2];
	int success = read(sockfd, acknowlegment, 1);
	if (success < 0) {
		printf("Error receiving acknowlegment: %s\n", strerror(errno));
		exit(0);
	} else if (acknowlegment[0] != 'S') {
		printf("Wrong acknowlegment message for sorted: %c\n", acknowlegment[0]);
		exit(0);
	}
}