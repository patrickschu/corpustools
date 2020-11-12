package corpustools

import (
	"fmt"
	"testing"
)

//Test  reading functionality

func TestReadCorpusLine(t *testing.T) {
	tt := []byte(`{"gilded":0,"author_flair_text":null,"score_hidden":false,"body":"Alright I'm done.","author":"gigaquack","score":3,"link_id":"t3_5yba3","name":"t1_c0299az","retrieved_on":1427426409,"author_flair_css_class":null,"subreddit":"reddit.com","edited":false,"controversiality":0,"ups":3,"parent_id":"t1_c0299ax","created_utc":"1192450691","archived":true,"downs":0,"subreddit_id":"t5_6","id":"c0299az","distinguished":null}`)
	res := ReadCorpusLine(tt)
	fmt.Println(res)
	if res.author != "gigaquack" {
		t.Errorf("Incorrect author, received %s", res.author)
	}
	if res.body != "Alright I'm done." {
		t.Errorf("Incorrect body, received %s", res.body)
	}
	if res.created_utc != "1192450691" {
		t.Errorf("Incorrect timestamp, received %s", res.created_utc)
	}
}

func TestReadCorpusFile(t *testing.T) {
 	inpath := "/home/patrick/Documents/morph/corpustools/resources/RC_2007-10"
 	res := ReadCorpusFile(inpath)
 	if len(res) != 150429 {
 		t.Errorf("Incorrect number of post, received %d, want 150429", len(res))
 	}
 	// {c0299an bostich test 1192450635}
 	res1 := res[0]
 	if res1.author != "bostich" {
 		t.Errorf("Incorrect author, received %s", res1.author)
 	}
 	if res1.body != "test" {
 		t.Errorf("Incorrect body, received %s", res1.body)
 	}
 	if res1.created_utc != "1192450635" {
 		t.Errorf("Incorrect timestamp, received %s", res1.created_utc)
 	}
 }

func TestWalkFolder(t *testing.T) {
	inpath := "/home/patrick/Documents/corpora/reddit_data"
	res, err := WalkFolder(inpath)
	if err != nil {
		t.Errorf("Walkfolder returned error %v", err)
	}
	expected := 93
	if len(res) != expected {
		t.Errorf("Incorrect number of files from %s, received %d, want %d", inpath, len(res), expected)
	}
}

func TestGetSubfolders(t *testing.T) {
	inpath := "/home/patrick/Documents/corpora/reddit_data"
	res, err := GetSubFolders(inpath)
	if err != nil {
		t.Errorf("GetSubFolders returned error %v", err)
	}
	expected := 10
	if len(res) != expected {
		t.Errorf("Incorrect number of folders from %s, received %d, want %d", inpath, len(res), expected)
	}
}

func TestReadCorpusFolder(t *testing.T) {
	inpath := "/home/patrick/Documents/morph/corpustools/resources"
	res := ReadCorpusFolder(inpath)
	if res == nil {
		t.Errorf("Error %v", res)
	}

}

func TestReadCorpus(t *testing.T) {
	inpath := "/home/patrick/Documents/morph/corpustools/resources"
	res := ReadCorpus(inpath)
	if res == nil {
		t.Errorf("Error %v", res)
	}

}

func TestBzipReader(t *testing.T) {
	inpath := "/home/patrick/Documents/morph/corpustools/resources/RC_2007-12.bz2"
	posts := ReadBzipCorpusFile(inpath)
	if len(posts) != 363390 {
		t.Errorf("Expected %d posts from %s, received %d", 363390, inpath, len(posts))
	}
}

func TestReadCorpusFile2(t *testing.T) {
	inpath := "/home/patrick/Documents/morph/corpustools/resources/RC_2007-12.bz2"
	posts := ReadCorpusFile(inpath)
	if len(posts) != 363390 {
		t.Errorf("Expected %d posts from %s, received %d", 363390, inpath, len(posts))
	}
}
