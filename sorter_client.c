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
	printf("TODO: Include all semaphore lines from last project, BUT FOR NUMBER OF CSVS READIN AND PROCESSED, NOT SORTED\n");	
	if (argc<7) {
		printf("Missing arguments\n");
		printf("Usage: ./sorter_client -c <column> -h <hostname or ip address> -p <port number> [-d <dirname>] [-o <output dirname>]\n");
		exit(1);
	} else if (argc % 2 != 1) {
		printf("Usage: ./sorter_client -c <column> -h <hostname or ip address> -p <port number> [-d <dirname>] [-o <output dirname>]\n");
		exit(1);
	}
	
	char *column = NULL;
	char *hostname = NULL;
	char *portNumber = NULL;
	char *directoryName = NULL;
	char *outputDirectoryName = NULL;
	
	int i;
	for (i=1;i<argc;i+=2)
	{
		printf("Argc=%d is %s \n", i, argv[i]);
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
	sem_init(&openedFiles, 0, maxOpenedFileLimit);
	
	
	parseAndSendDir(hostname, portNumber, directoryName, column);
	
	int sockfd = createSocket(hostname, portNumber);
	sendRequest(sockfd, getDump, column, NULL);
	struct csv *readDump(int sockfd);
	
	// call dump here and output file to the specified output directory
	// already declared before in this context as dir
	
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
			}
			else 
			{
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
	
	printf("%lu ", pthread_self());
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
	
	printf("Sent Request to Server.\n");
	printf("Awknowledgement Received?\n");
	
	close(sockfd);
	
	free(arguments);
	int retval = 1;
	pthread_exit((void *) (intptr_t) retval);
	sem_post(&openedFiles);
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
	
	/* use sendRequest to send a sort/dump request and you can use readDump to get the merged CSV file after your dump request.*/
	return sockfd;
} // close(sockfd);







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
		exit(1);
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


// int sortFile(char *inputDir, char *outputDir, char *fileName, char *sortBy)
// {

// 	FILE *in;
// 	if (inputDir != NULL) 
// 	{
// 		char *inputLocation = calloc(1, (strlen(inputDir) + strlen(fileName) + 2) * sizeof(char)); 
// 		strcat(inputLocation, inputDir);
// 		strcat(inputLocation, "/");
// 		strcat(inputLocation, fileName);
// 		in = fopen(inputLocation, "r");
// 		free(inputLocation);
// 	} 
// 	else 
// 	{
// 		in = fopen(fileName, "r");
// 	}
	
// 	// remove .csv from the name
// 	char *fileNameWithoutCSV = (char *) malloc((strlen(fileName)-3)*sizeof(char));
// 	memcpy(fileNameWithoutCSV, fileName, (strlen(fileName)-4));
// 	fileNameWithoutCSV[(strlen(fileName)-4)] = '\0';
	
// 	// outputFilename = filename-sorted-[sortby].csv
// 	char* outputFilename = calloc(1, (strlen(fileNameWithoutCSV) + strlen("-sorted-") + strlen(sortBy) + strlen(".csv") + 1) * sizeof(char));
// 	strcat(outputFilename, fileNameWithoutCSV);
// 	strcat(outputFilename, "-sorted-");
// 	strcat(outputFilename, sortBy);
// 	strcat(outputFilename, ".csv");

// 	free(fileNameWithoutCSV);

// 	struct csv *csv = parseCSV(in);
	
// 	//char *sortBy = argv[2];
// 	//!!code changed to handle query that has mutliple sort by values, comma separated
// 	//array of strings
// 	char **columnNames = csv->columnNames;
	
// 	//find the indexes of the desired field to sort by; color = 0, director_name = 1 ...
// 	int numberOfSortBys = 1;
// 	int i; 
// 	char *query = sortBy;
// 	for (i=0; query[i]!='\0'; i++) 
// 	{
// 		if (query[i] == ',') 
// 		{
// 			numberOfSortBys += 1;
// 		}
// 	}
	
// 	//all the sortBy values separated
// 	char **arrayOfSortBys = (char **)malloc(numberOfSortBys * sizeof(char *));
// 	int counter = 0;

	
// 	//parse out the different sortBy values
// 	char *temp = query;
// 	for (i=0; query[i]!='\0'; i++) 
// 	{
// 		if (query[i] == ',') 
// 		{
// 			char *sortVal = (char *) malloc((&(query[i])-temp+1) * sizeof(char));
// 			memcpy(sortVal, temp, (&(query[i])-temp));
// 			sortVal[&(query[i])-temp] = '\0';
// 			arrayOfSortBys[counter] = sortVal;
// 			counter++;
// 			temp=&(query[i])+1;
// 		}
// 	}
	
// 	//for the last value after the last comma
// 	char *sortVal = (char *) malloc((&(query[i])-temp+1) * sizeof(char));
// 	memcpy(sortVal, temp, (&(query[i])-temp));
// 	sortVal[&(query[i])-temp] = '\0';
// 	arrayOfSortBys[counter] = sortVal;
// 	//printf("sortVal: %s\n", sortVal);
// 	int *indexesOfSortBys = (int *) malloc(numberOfSortBys * sizeof(int));
// 	int j;
// 	for (i=0; i<numberOfSortBys; i++) 
    
// 	{
// 		for (j=0; j < columns; j++) 
// 		{
// 			//printf("strcmp %s with %s\n", columnNames[i], arrayOfSortBys[counter]);
// 			if (strcmp(columnNames[j], arrayOfSortBys[i])==0) 
// 			{
// 				indexesOfSortBys[i] = j;
// 				break;
// 			}
// 		}
// 		//check if header is found
// 		if (j == columns) 
// 		{
// 			printf("Error, could not find query in column names\n");
// 			exit(0);
// 		}
// 	}
	
// 	//free the parsed character array of query
// 	for (i=0; i<numberOfSortBys; i++) 
// 	{
// 		free(arrayOfSortBys[i]);
// 	}
// 	free(arrayOfSortBys);
// 	//sorts csv by sortBy
	
// 	mergesortMovieList(csv, indexesOfSortBys, csv->columnTypes, numberOfSortBys);
	
// 	free(indexesOfSortBys);
// 	fclose(in);
// 	return 1;
// }

struct csv *mergeCSVs(struct csv **csvs, unsigned int size, char *sortBy) 
{	
	int i;
	//Array of pointers to row in every csv file.
	int *positions = calloc(1, sizeof(int) * size);
	int lowestPosition;

	if (size == 0) {
		printf("No CSV files found in directory.\n");
		return 0;
	}

	//find the indexes of the desired field to sort by; color = 0, director_name = 1 ...
	int numberOfSortBys = 1;
	char *query = sortBy;
	for (i=0; query[i]!='\0'; i++) 
	{
		if (query[i] == ',') 
		{
			numberOfSortBys += 1;
		}
	}
	
	//all the sortBy values separated
	char **arrayOfSortBys = (char **)malloc(numberOfSortBys * sizeof(char *));
	int counter = 0;

	
	//parse out the different sortBy values
	char *temp = query;
	for (i=0; query[i]!='\0'; i++) 
	{
		if (query[i] == ',') 
		{
			char *sortVal = (char *) malloc((&(query[i])-temp+1) * sizeof(char));
			memcpy(sortVal, temp, (&(query[i])-temp));
			sortVal[&(query[i])-temp] = '\0';
			arrayOfSortBys[counter] = sortVal;
			counter++;
			temp=&(query[i])+1;
		}
	}
	
	//for the last value after the last comma
	char *sortVal = (char *) malloc((&(query[i])-temp+1) * sizeof(char));
	memcpy(sortVal, temp, (&(query[i])-temp));
	sortVal[&(query[i])-temp] = '\0';
	arrayOfSortBys[counter] = sortVal;
	//printf("sortVal: %s\n", sortVal);
	int *indexesOfSortBys = (int *) malloc(numberOfSortBys * sizeof(int));
	int j;
	for (i=0; i<numberOfSortBys; i++) 
	{
		for (j=0; j < columns; j++) 
		{
			//printf("strcmp %s with %s\n", columnNames[i], arrayOfSortBys[counter]);
			if (strcmp(csvs[0]->columnNames[j], arrayOfSortBys[i])==0) 
			{
				indexesOfSortBys[i] = j;
				break;
			}
		}
		//check if header is found
		if (j == columns) 
		{
			printf("Error, could not find query in column names\n");
			exit(0);
		}
	}
	
	//free the parsed character array of query
	for (i=0; i<numberOfSortBys; i++) 
	{
		free(arrayOfSortBys[i]);
	}
	free(arrayOfSortBys);
	


	struct csv *mergedCSV = malloc(sizeof(struct csv));
	mergedCSV->columnNames = malloc(sizeof(char *) * columns);
	mergedCSV->columnTypes = malloc(sizeof(enum type) * columns);
	mergedCSV->entries = malloc(sizeof(struct entry *) * maxEntries * size);
	mergedCSV->numEntries = 0;
	//Use column names and column types from first csv file.
	for (i=0;i<columns;i++) {
		mergedCSV->columnNames[i] = malloc(sizeof(char) * maxStringSize);
		strcpy(mergedCSV->columnNames[i], csvs[0]->columnNames[i]);
		mergedCSV->columnTypes[i] = csvs[0]->columnTypes[i];
	}

	while(!endPositionsReached(csvs, positions, size)) {
		lowestPosition = -1;
		for (i = 0 ; i < size ; i++) {
			if (!endPositionReached(csvs[i], positions[i]) && (lowestPosition == -1 ||compareValue(csvs[lowestPosition]->entries[positions[lowestPosition]], csvs[i]->entries[positions[i]], csvs[i]->columnTypes, indexesOfSortBys, numberOfSortBys) == 1)) {
				lowestPosition = i;
			}
		}
		mergedCSV->entries[mergedCSV->numEntries++] = copyEntry(csvs[lowestPosition]->entries[positions[lowestPosition]]);
		positions[lowestPosition]++;

	}

	free(indexesOfSortBys);

	return mergedCSV;

}

int endPositionsReached(struct csv **csvs, int *positions, unsigned int size) {
	int i;

	for (i=0;i<size; i++) {
		if (!endPositionReached(csvs[i], positions[i])) {
			return 0;
		}
	}

	return 1;
}

int endPositionReached(struct csv *csv, int position) {
	return csv->numEntries == position;
}

struct entry *copyEntry(struct entry *src) {
	struct entry *ret = malloc(sizeof(struct entry));
	ret->values = malloc(sizeof(union value) * columns);
	int i;

	for (i = 0 ; i < columns ; i++) {
		ret->values[i] = src->values[i];
	}

	return ret;
}

void mergesortMovieList(struct csv *csv, int *indexesOfSortBys, enum type *columnTypes, int numberOfSortBys) 
{
	
	struct entry** entries = csv->entries;
	long low = 0;
	//numEntries includes the labels row (-1), to use Array indicies (-1)
	long high = csv->numEntries-1;
	
	//start mergeSort
	mergeSort(low, high, entries, columnTypes, indexesOfSortBys, numberOfSortBys);
	
}

void mergeSort(long low, long high, struct entry** entries, enum type *columnTypes, int *compareIndexes, int numberOfSortBys)
{
	//split up array until single blocks are made
	if (low < high){
		//lower array has the "mid" element
		mergeSort(low, ((low+high)/2), entries, columnTypes, compareIndexes, numberOfSortBys);
		mergeSort(((low+high)/2)+1, high, entries, columnTypes, compareIndexes, numberOfSortBys);
		mergeParts(low, high, entries, columnTypes, compareIndexes, numberOfSortBys);
	}
	return;
}

void mergeParts(long low, long high, struct entry** entries, enum type *columnTypes, int *compareIndexes, int numberOfSortBys)
{
	// (low+high)/2 is part of the lower array
	long  mid = (low+high)/2;
	
	//dynamically create an array of pointers
	struct entry **tempArray1;
	struct entry **tempArray2;
	
	//copy the pointers from entries into tempArray1 and tempArray2
	tempArray1 = malloc(sizeof(struct entry *)*(mid-low+1)); 
	tempArray2 = malloc(sizeof(struct entry *)*(high-mid));
	int i;
	for (i=0; i<mid-low+1; i++)
	{
		tempArray1[i] = entries[low+i];
	}
	for (i=0; i<high-mid; i++)
	{
		tempArray2[i] = entries[mid+1+i];
	}
	
	//check if memory was not properly allocated by malloc
	if (tempArray1==NULL || tempArray2==NULL)
	{
		printf("Error in allocation of memory\n");
		exit(0);
	}
	
	// insertLocation is the location in entries that will be overwritten
	long insertLocation = low;
	//for the first temporary array
	long index1 = low; 
	//for the second temporary array
	long index2 = mid+1; 
	while (index1 <= mid && index2 <= high) 
	{ //the lower array gets the middle element
		//compare succeeding elements in tempArray1 vs succeeding elements in tempArray2
		//dereference tempArray(1,2) at an index, dereference and grab values, dereference and grab string, decimal, or float value
		//compareValue returns -1 when element in tempArray1 is smaller and 1 whenelement in tempArray2 is bigger
		if (compareValue((tempArray1[index1-low]), (tempArray2[index2-(mid+1)]), columnTypes, compareIndexes, numberOfSortBys) == -1) 
		{
			//if tempArray1 has the smaller value or they're equal: this makes merge sort stable
			entries[insertLocation] = tempArray1[index1-low];
			index1++;
		} 
		else 
		{ //if tempArray2  has the smalller value
			entries[insertLocation] = tempArray2[index2-(mid+1)];
			index2++;
		}
		//insertLocation will never go out of bounds because the loop stops when all the values put back into entries from low to high 
		insertLocation++;
	}
	
	//if tempArray1 still has more entries, put at the end of entries
	while (index1 <= mid) 
	{
		entries[insertLocation] = tempArray1[index1-low];
		index1++;
		insertLocation++;
	}

	//if tempArray2 still has more entries, put at the end of entries
	while (index2 <= high) 
	{
		entries[insertLocation] = tempArray2[index2-(mid+1)];
		index2++;
		insertLocation++;
	}
	
	free(tempArray1);
	free(tempArray2);
	
	return;
} 

int compareValue(struct entry *tempArray1, struct entry *tempArray2, enum type *columnTypes, int *compareIndexes, int numberOfSortBys) 
{
	//the values could be string, integer, or decimal
	int counter = 0;
	union value *location1;
	union value *location2;
	enum type dataType;
	int temp=0;

	//printf("compareIndexes[0] = %d", compareIndexes[0]);
	//printf("columnTypes[0] = %d", columnTypes[0]);

	while (counter < numberOfSortBys)
	{

		//printf("Comparing Index %d\n", compareIndexes[counter]);

		location1 = &(tempArray1->values[compareIndexes[counter]]);
		location2 = &(tempArray2->values[compareIndexes[counter]]);
		dataType = columnTypes[compareIndexes[counter]];
		if (dataType == string) 
		{
			temp = strcmp(location1->stringVal,location2->stringVal);
			if (temp < 0) 
			{
				return -1; //first value is smaller or equal
			} 
			else if (temp > 0) 
			{
				return 1; //first value is bigger
			} 
			else 
			{
				counter++;
				continue;
			}
		} 
		else if (dataType == integer) 
		{
			temp = (location1->intVal) - (location2->intVal);
			if (temp < 0) 
			{
				return -1; //first value is smaller or equal
			} 
			else if (temp > 0) 
			{
				return 1; //first value is bigger
			} 
			else 
			{
				counter++;
				continue;
			}
		} 
		else if (dataType == decimal) 
		{
			temp = (location1->decimalVal) - (location2->decimalVal);
			if (temp < 0) 
			{
				return -1; //first value is smaller or equal
			} 
			else if (temp > 0) 
			{
				return 1; //first value is bigger
			} 
			else 
			{
				counter++;
				continue;
			}
		} 
		else 
		{
			printf("Error: compareValue: %d\n", dataType);
			exit(-1);
		}
	}
	return -1; //Both values are exactly the same ==> first value is smaller!! since mergeSort is stable
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
	for (i=0;i<maxEntries;i++) 
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

	struct csv *ret = malloc(sizeof(struct csv));
	ret->columnNames = malloc(sizeof(char *) * columns);
	ret->columnTypes = malloc(sizeof(enum type) * columns);

	int success, i, j;

	//Read in column names.
	for (i=0;i<columns;i++) {
		ret->columnNames[i] = malloc(sizeof(char) * 51);
		bzero(ret->columnNames[i], 51);
		success = read(sockfd, ret->columnNames[i], 50);
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


	//Read in all csv values.
	for (i=0;i<ret->numEntries;i++) {
		ret->entries[i] = malloc(sizeof(struct entry));
		ret->entries[i]->values = malloc(sizeof(union value) * columns);
		for (j=0;j<columns;j++) {
			if (ret->columnTypes[j] == integer) {
				success = read(sockfd, &(ret->entries[i]->values[j].intVal), sizeof(long));
			} else if (ret->columnTypes[j] == decimal) {
				success = read(sockfd, &(ret->entries[i]->values[j].decimalVal), sizeof(double));
			} else if (ret->columnTypes[j] == string) {
				size_t stringLength;
				success = read(sockfd, &stringLength, sizeof(size_t));
				char *stringValue = malloc(sizeof(char) * stringLength);
				success = read(sockfd, stringValue, stringLength);
				ret->entries[i]->values[j].stringVal = stringValue;
			} else {
				printf("Unknown data type found in column %d!\n", j);
				exit(0);
			}

			if (success < 0) {
				printf("Error Reading in a value.\n");
				exit(0);
			}

		}
	}

	return ret;

}

void sendCSV(int sockfd, struct csv *csv) {
	int i, j, success;
	char columnName[50];
	bzero(columnName, 50);

	//Print Columns (Write 50 characters at once).
	for (i=0 ; i < columns ; i++) {
		strcpy(columnName, csv->columnNames[i]);
		success = write(sockfd, columnName, 50);
	}


	if (success <= 0) {
		printf("Writing column names failed.\n");
		exit(0);
	}

	//Print Column Types ('S', 'I', or 'D')
	for (i=0;i < columns ; i++) {
		if (csv->columnTypes[i] == string) {
			success = write(sockfd, "S", 1);
		} else if (csv->columnTypes[i] == integer) {
			success = write(sockfd, "I", 1);
		} else if (csv->columnTypes[i] == decimal) {
			success = write(sockfd, "D", 1);
		} else {
			printf("Invalid Column Type Found: %d\n", csv->columnTypes[i]);
		}
	}

	if (success <= 0) {
		printf("Writing column types failed.\n");
		exit(0);
	}

	//Print number of entries in CSV.
	write(sockfd, &(csv->numEntries), sizeof(int));
	size_t length;
	for (i=0;i<csv->numEntries;i++) {
		for (j=0;j<columns;j++) {
			if (csv->columnTypes[j] == string) {
				length = strlen(csv->entries[i]->values[j].stringVal);
				success = write(sockfd, &length, sizeof(size_t));
				success = write(sockfd, csv->entries[i]->values[j].stringVal, strlen(csv->entries[i]->values[j].stringVal));
			} else if (csv->columnTypes[j] == integer) {
				success = write(sockfd, &(csv->entries[i]->values[j].intVal), sizeof(long));
			} else if (csv->columnTypes[j] == decimal) {
				success = write(sockfd, &(csv->entries[i]->values[j].decimalVal), sizeof(double));
			} else {
				printf("Invalid Column Type Found: %d\n", csv->columnTypes[i]);
			}
		}
	}

	if (success <= 0) {
		printf("Writing CSV Entries failed.\n");
		exit(0);
	}
	
}

//If requesting dump, csv should be NULL.
void sendRequest(int sockfd, enum requestType type, char *sortBy, struct csv *csv) {
	int success;

	//Send Request Type.
	if (type == sort) {
		success = write(sockfd, "S", 1);
	} else if (type == getDump) {
		printf("%d\n", errno);
		success = write(sockfd, "D", 1);
		printf("%d\n", errno);
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