//Constants

const char *stringValues[] = {"color", "director_name", "actor_2_name", "genres", "actor_1_name", "movie_title", "actor_3_name", "plot_keywords", "movie_imdb_link", "language", "country", "content_rating"};
const unsigned int stringValuesSize = 12;
const char *intValues[] = {"num_critic_for_reviews", "duration", "director_facebook_likes", "actor_3_facebook_likes", "actor_1_facebook_likes", "gross", "num_voted_users", "cast_total_facebook_likes", "facenumber_in_poster", "num_user_for_reviews", "budget", "title_year", "actor_2_facebook_likes", "movie_facebook_likes"};
const unsigned int intValuesSize = 14;
const char *doubleValues[] = {"imdb_score", "aspect_ratio"};
const unsigned int doubleValuesSize = 2;

//Number of columns in CSV file.
const unsigned int columns = 28;

//Maximum allowed number of characters for input through scanf.
unsigned int maxStringSize = 5000;

//Maximum allowed number of movies for input through scanf.
unsigned int maxEntries = 32768;

//Maximum allowed number of input files.
unsigned int fileCap = 8192;

unsigned int currentFile = 0;


//unions and structs
union value 
{
	long intVal;
	double decimalVal;
	char *stringVal;
};

struct entry 
{
	union value *values;
};

struct csv 
{
	char **columnNames;
	enum type *columnTypes;
	struct entry **entries;
	int numEntries;
};

enum type 
{
	string,
	integer,
	decimal,
	error = -1
};

enum requestType {
	sort, 
	getDump
};

struct headerInfo 
{
	enum type *types;
	char **columnNames;
};

struct entryInfo 
{
	struct entry **entries;
	int numEntries;
};

struct sortFileArguments 
{
	char *inputDir;
	char *outputDir;
	char *fileName;
	char *sortBy;
};

struct sortDirArguments 
{
	char *subDir;
	char *outputDir;
	char *sortBy;
};

struct request
{
	enum requestType type; //Request Type must be supplied.
	char *sortBy; //Column to sort by
	struct csv *csv; //CSV File to sort - not null if and only if request is sortFile.

};

//CSV parsing methods
struct csv *parseCSV(FILE *file);
struct headerInfo getHeaderInfo(FILE *file);
struct entryInfo getCSVEntries(FILE *file, enum type *columnTypes);
enum type getTypeFromColumnName(char *name);

//included for parsing "recursively" through subdirectories
int parseDir(char *inputDir, char *outputDir, char *sortBy);

int isCSV(char *fname);

void *threadExecuteSortFile(void *args);

int sortFile(char *inputDir, char *outputDir, char *fileName, char *sortBy);

//Sorting Method: Merges CSV Files.
struct csv *mergeCSVs(struct csv **csvs, unsigned int size, char *sortBy);

//Sorting method: setup variables
void mergesortMovieList(struct csv *csv, int *indexesOfSortBys, enum type *columnTypes, int numberOfSortBys);

//Sorting method: recursive call, splits up array
void mergeSort(long low, long high, struct entry** entries, enum type *columnTypes, int *compareIndexes, int numberOfSortBys);

//Sorting method: regrouping
void mergeParts(long low, long high, struct entry** entries, enum type *columnTypes, int *compareIndexes, int numberOfSortBys);

//Comparing Values in an entry
int compareValue(struct entry *tempArray1, struct entry *tempArray2, enum type *columnTypes, int *compareIndexes, int numberOfSortBys);

//Output methods
void printCSV(struct csv *csv, FILE *file);

//Cleanup methods
void freeCSV(struct csv *csv);

//Utility Methods
char *addCharacterToString(char *string, char next, int position, int *localMaxStringSize);
char *addStringToString(char *string, char *next, int *position, int *localMaxStringSize);
struct entry **addEntryToArray(struct entry **array, struct entry *entry, int position);
void setValue(union value *location, char *value, enum type dataType);
int endPositionsReached(struct csv **csvs, int *positions, unsigned int size);
int endPositionReached(struct csv *csv, int position);
struct entry *copyEntry(struct entry *src);


//Network Methods
struct request readRequest(int sockfd);
void sendDump(int sockfd, struct csv *mergedCSV);