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

struct csv **files;
sem_t openedFiles;
const unsigned int maxOpenedFileLimit = 1000;

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

struct sendFileArguments 
{
	char *host;
	char *portNumber;
	char *inputDir;
	char *fileName;
	char *sortBy;
};

enum requestType {
	sort, 
	getDump
};

//CSV parsing methods
struct csv *parseCSV(FILE *file);
struct headerInfo getHeaderInfo(FILE *file);
struct entryInfo getCSVEntries(FILE *file, enum type *columnTypes);
enum type getTypeFromColumnName(char *name);

// goes through, creates an individual socket and sends file to server, does not recieve anything
int parseAndSendDir(char *host, char *portNumber, char *inputDir, char *sortBy);

int isCSV(char *fname);

void *threadSendFile(void *args);

//Output methods
void printCSV(struct csv *csv, FILE *file);

//Cleanup methods
void freeCSV(struct csv *csv);

//Utility Methods
char *addCharacterToString(char *string, char next, int position, int *localMaxStringSize);
struct entry **addEntryToArray(struct entry **array, struct entry *entry, int position);
void setValue(union value *location, char *value, enum type dataType);

//Network Methods
struct csv *readDump(int sockfd);
void sendCSV(int sockfd, struct csv *csv);
void sendRequest(int sockfd, enum requestType type, char *sortBy, struct csv *csv);
void readAcknowledgement(int sockfd);
int createSocket(char *hostname, char *portNumber);


int forceRead(int sockfd, void *location, size_t size);