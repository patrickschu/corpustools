package corpustools

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

// Read the Reddit file

/*
{"gilded":0,"author_flair_text":null,"score_hidden":false,"body":"Alright I'm done.","author":"gigaquack","score":3,"link_id":"t3_5yba3","name":"t1_c0299az","retrieved_on":1427426409,"author_flair_css_class":null,"subreddit":"reddit.com","edited":false,"controversiality":0,"ups":3,"parent_id":"t1_c0299ax","created_utc":"1192450691","archived":true,"downs":0,"subreddit_id":"t5_6","id":"c0299az","distinguished":null}

KEYS:

{'author', 'gilded', 'distinguished', 'subreddit_id', 'retrieved_on',
'parent_id', 'body', 'created_utc',
'link_id', 'author_flair_css_class', 'id',
'controversiality', 'score_hidden', 'ups', 'edited',
'score', 'author_flair_text', 'downs', 'name', 'subreddit', 'archived'}
*/

//Post collects all relevant info from Reddit post
//Struct attributes mirror the file
type Post struct {
	id          string //unique ID
	author      string
	body        string //text of post
	created_utc string
}

//Read the data from the line from JSON
func ReadCorpusLine(inline []byte) Post {
	var linedata map[string]string
	json.Unmarshal(inline, &linedata)
	post := Post{linedata["id"],
		linedata["author"],
		linedata["body"],
		linedata["created_utc"]}
	return post
}


// Walk over files in `RootDir`
func WalkFolder(RootDir string) ([]string, error) {
	files := []string{}
	err := filepath.Walk(RootDir, func(fpath string, finfo os.FileInfo, err error) error {
		if !finfo.IsDir() {
			files = append(files, fpath)
		}
		return nil
	})
	log.Printf("WalkFolder found %d files", len(files))
	return files, err
}

// Walk over files in `RootDir`
func GetSubFolders(RootDir string) ([]string, error) {
	folders := []string{}
	err := filepath.Walk(RootDir, func(fpath string, finfo os.FileInfo, err error) error {
		if finfo.IsDir() {
			folders = append(folders, fpath)
		}
		return nil
	})
	return folders, err
}


// ReadCorpusFolder reads all files in `infolder` to Post objects
func ReadCorpusFolder(Infolder string) []Post {
	posts := []Post{}
	// Find all files
	inputFiles, err := WalkFolder(Infolder)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Found %d files in Infolder %s", len(inputFiles), Infolder)
	// Iterate over
	for ind, infile := range inputFiles {
		log.Printf("Working on file %s", infile)
		// different treatment for txt v bzip2 files
		posts = append(posts, ReadCorpusFile(infile)...)
		log.Printf("Processed %d files", ind)
	}
	return posts
}

// Read all files in Infolder and subfolders into Posts for corpus
func ReadCorpus(Infolder string) []Post {
	posts := []Post{}
	subfolders, err := GetSubFolders(Infolder)
	if err != nil {
		log.Printf("Error %s", err)
	}
	log.Printf("Found %d files in Infolder %s", len(subfolders), Infolder)
	// Iterate over
	for ind, infile := range subfolders {
		log.Printf("Working on file %s", infile)
		posts = append(posts, ReadCorpusFolder(infile)...)
		log.Printf("Processed %d folders", ind)
	}
	return posts
}

// ReadCorpusTxtFile reads file where each line represents a JSON Reddit post
func ReadCorpusTxtFile(inPath string) []Post {
	var posts []Post
	file, err := os.Open(inPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		//create new post here
		post := ReadCorpusLine(scanner.Bytes())
		posts = append(posts, post)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return posts
}

// Identical to ReadCorpusTxtFile, but takes bzip2ed txt file as input
func ReadBzipCorpusFile(inPath string) []Post {
	var posts []Post
	file, err := os.Open(inPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	unzipreader := bzip2.NewReader(file)
	scanner := bufio.NewScanner(unzipreader)
	for scanner.Scan() {
		post := ReadCorpusLine(scanner.Bytes())
		posts = append(posts, post)
	}
	return posts
}

// ReadCorpusFile returns Posts from corpus files, either txt or bzipped files
func ReadCorpusFile(inPath string) []Post {
	posts := []Post{}
	fileExt := filepath.Ext(inPath)
	switch fileExt {
		case "":
			posts = append(posts, ReadCorpusTxtFile(inPath)...)
		case ".bz2":
			posts = append(posts, ReadBzipCorpusFile(inPath)...)
		default:
		}
	return posts
}
