PROJ=dzcp
CC=gcc

all: clean $(PROJ)

$(PROJ):
	$(CC) -Wall $(PROJ).c -o $(PROJ)
clean:
	rm -rf $(PROJ) *.o
