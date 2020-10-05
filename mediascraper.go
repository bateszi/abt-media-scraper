package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type AppConfig struct {
	Db DbConfig `json:"db"`
}

type DbConfig struct {
	User     string `json:"user"`
	Password string `json:"pass"`
	Server   string `json:"server"`
	DbName   string `json:"dbName"`
}

type AniListSeasonPage struct {
	Page    int
	PerPage int
}

type AniListSeason struct {
	Season  string
	Year    int
	IsAdult bool
	Results []AniListResultsPage
}

type GraphQlReqBody struct {
	Query     string         `json:"query"`
	Variables GraphQlReqVars `json:"variables"`
}

type GraphQlReqVars struct {
	PageNo     int    `json:"pageNo"`
	PerPage    int    `json:"perPage"`
	Season     string `json:"season"`
	SeasonYear int    `json:"seasonYear"`
	IsAdult    bool   `json:"isAdult"`
}

type AniListDate struct {
	Year  int
	Month int
	Day   int
}

type AniListMediaTitle struct {
	Romaji        string
	English       string
	Native        string
	UserPreferred string
}

type AniListMediaItem struct {
	Id        int
	Title     AniListMediaTitle
	Synonyms  []string
	StartDate AniListDate
	Genres    []string
}

type AniListPageInfo struct {
	Total       int
	CurrentPage int
	LastPage    int
	HasNextPage bool
	PerPage     int
}

type AniListPage struct {
	PageInfo AniListPageInfo
	Media    []AniListMediaItem
}

type AniListPageContainer struct {
	Page AniListPage
}

type AniListData struct {
	Data AniListPageContainer
}

type AniListResultsPage struct {
	Page AniListData
}

var httpRequests int

func kill(context string, err error) {
	fmt.Println("unrecoverable error", context, err.Error())
	panic(err)
}

func maim(context string, err error) {
	fmt.Println("encountered error", context, err.Error())
}

func updateMediaGenres(db *sql.DB, pkMediaId int64, mediaItem AniListMediaItem) {
	genres := mediaItem.Genres

	stmt, err := db.Prepare("DELETE FROM `media_genres` WHERE (`fk_media_id` = ?)")
	if err != nil {
		maim("could not prepare sql statement for deleting stale media genres", err)
	}
	_, err = stmt.Exec(
		pkMediaId,
	)
	if err != nil {
		maim("could not delete stale media genres", err)
	}

	for i := range genres {
		genre := strings.TrimSpace(genres[i])

		var pkGenreId int64
		err := db.QueryRow("SELECT pk_genre_id FROM genres WHERE genre = ?", genre).Scan(&pkGenreId)
		if err != nil && err.Error() != "sql: no rows in result set" {
			maim("could not run query to find media", err)
		}

		if pkGenreId == 0 {
			stmt, err := db.Prepare("INSERT INTO `genres` " +
				"(`genre`) " +
				"VALUES (?)")
			if err != nil {
				maim("could not prepare sql statement for inserting into genres table", err)
			}
			insertedGenre, err := stmt.Exec(
				genre,
			)
			if err != nil {
				maim("could not insert genre into db", err)
			}

			pkGenreId, err = insertedGenre.LastInsertId()
			if err != nil {
				maim("could not get genre id from LastInsertId() call", err)
			}

			fmt.Println("inserted genre", genre)
		}

		stmt, err := db.Prepare("INSERT INTO `media_genres` " +
			"(`fk_media_id`, `fk_genre_id`) " +
			"VALUES (?, ?)")
		if err != nil {
			maim("could not prepare to insert media genres", err)
		}
		_, err = stmt.Exec(
			pkMediaId,
			pkGenreId,
		)
		if err != nil {
			maim("could not insert media genres", err)
		}

		fmt.Println("Inserted genre for media", genre, mediaItem.Title.UserPreferred)
	}
}

func updateMediaTitles(db *sql.DB, pkMediaId int64, mediaItem AniListMediaItem) {
	mediaTitles := make([]string, 0)
	mediaTitles = append(mediaTitles, mediaItem.Title.English, mediaItem.Title.Native, mediaItem.Title.Romaji)

	stmt, err := db.Prepare("DELETE FROM `media_titles` WHERE (`fk_media_id` = ?)")
	if err != nil {
		maim("could not prepare sql statement for deleting stale media_titles", err)
	}
	_, err = stmt.Exec(
		pkMediaId,
	)
	if err != nil {
		maim("could not delete stale media_titles", err)
	}

	insertQuery := "INSERT INTO `media_titles` " +
		"(`fk_media_id`, `title`) " +
		"VALUES "
	var insertParams []interface{}

	for i := range mediaTitles {
		mediaTitle := strings.TrimSpace(mediaTitles[i])

		if len(mediaTitle) == 0 {
			continue
		}

		insertQuery += "(?, ?),"
		insertParams = append(insertParams, pkMediaId, mediaTitle)
	}

	insertQuery = strings.TrimSuffix(insertQuery, ",")
	stmt, err = db.Prepare(insertQuery)
	if err != nil {
		maim("could not prepare to insert media_titles", err)
	}
	_, err = stmt.Exec(insertParams...)
	if err != nil {
		maim("could not insert media_titles", err)
	}
}

func updateMediaSynonyms(db *sql.DB, pkMediaId int64, mediaItem AniListMediaItem) {
	synonyms := make([]string, 0)

	for k := range mediaItem.Synonyms {
		synonyms = append(synonyms, mediaItem.Synonyms[k])
	}

	if len(synonyms) == 0 {
		return
	}

	stmt, err := db.Prepare("DELETE FROM `media_synonyms` WHERE (`fk_media_id` = ?)")
	if err != nil {
		maim("could not prepare sql statement for deleting stale media synonyms", err)
	}
	_, err = stmt.Exec(
		pkMediaId,
	)
	if err != nil {
		maim("could not delete stale media synonyms", err)
	}

	insertSynonymQuery := "INSERT INTO `media_synonyms` " +
		"(`fk_media_id`, `synonym`) " +
		"VALUES "
	var insertSynonymParams []interface{}

	for i := range synonyms {
		synonym := strings.TrimSpace(synonyms[i])

		if len(synonym) == 0 {
			continue
		}

		insertSynonymQuery += "(?, ?),"
		insertSynonymParams = append(insertSynonymParams, pkMediaId, synonym)
	}

	insertSynonymQuery = strings.TrimSuffix(insertSynonymQuery, ",")
	stmt, err = db.Prepare(insertSynonymQuery)
	if err != nil {
		maim("could not prepare to insert media synonyms", err)
	}
	_, err = stmt.Exec(insertSynonymParams...)
	if err != nil {
		maim("could not insert media synonyms", err)
	}
}

func updateMedia(db *sql.DB, mediaItem AniListMediaItem) int64 {
	var pkMediaId int64
	err := db.QueryRow("SELECT pk_media_id FROM media WHERE guid = ?", mediaItem.Id).Scan(&pkMediaId)
	if err != nil && err.Error() != "sql: no rows in result set" {
		maim("could not run query to find media", err)
	}

	startYear := mediaItem.StartDate.Year
	startMonth := mediaItem.StartDate.Month
	if startMonth == 0 {
		startMonth = 1
	}
	startDay := mediaItem.StartDate.Day
	if startDay == 0 {
		startDay = 1
	}

	currentTimestamp := time.Now().Format("2006-01-02 15:04:05")

	startDateString := fmt.Sprintf("%d-%02d-%02d", startYear, startMonth, startDay)
	startDate, err := time.Parse("2006-01-02", startDateString)
	if err != nil {
		maim("could not parse start date for media", err)
	}

	mediaTitle := mediaItem.Title.UserPreferred

	if len(strings.TrimSpace(mediaItem.Title.English)) > 0 {
		mediaTitle = mediaItem.Title.English
	}

	if pkMediaId == 0 {
		stmt, err := db.Prepare("INSERT INTO `media` " +
			"(`title`, `start_date`, `created`, `modified`, `guid`) " +
			"VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			maim("could not prepare sql statement for inserting into media table", err)
		}
		insertedMedia, err := stmt.Exec(
			mediaTitle,
			startDate.Format("2006-01-02"),
			currentTimestamp,
			currentTimestamp,
			mediaItem.Id,
		)
		if err != nil {
			maim("could not insert media item into db", err)
		}

		pkMediaId, err = insertedMedia.LastInsertId()
		if err != nil {
			maim("could not get media item id from LastInsertId() call", err)
		}

		fmt.Println("inserted", mediaTitle)
	} else {
		stmt, err := db.Prepare("UPDATE `media` SET `title` = ?, `start_date` = ?, `modified` = ?, `guid` = ? WHERE (`pk_media_id` = ?)")
		if err != nil {
			maim("could not prepare sql statement for updating media table", err)
		}
		_, err = stmt.Exec(
			mediaTitle,
			startDate.Format("2006-01-02"),
			currentTimestamp,
			mediaItem.Id,
			pkMediaId,
		)
		if err != nil {
			maim("could not update media item in db", err)
		}

		fmt.Println("updated", mediaTitle)
	}

	return pkMediaId
}

func getAnimeSeasonByPage(seasonInfo *AniListSeason, pageInfo AniListSeasonPage) AniListResultsPage {
	const seasonPayloadQuery = `query ($pageNo: Int, $perPage: Int, $season: MediaSeason, $seasonYear: Int, $isAdult: Boolean) {
  Page(page: $pageNo, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
      perPage
    },
    media(season:$season, seasonYear:$seasonYear, isAdult:$isAdult) {
      id,
      title {
        romaji
        english
        native
        userPreferred
      },
	  synonyms,
      startDate {
        year
        month
        day
      },
      genres
    }
  }
}`
	seasonPageGQLBody := GraphQlReqBody{
		Query: seasonPayloadQuery,
		Variables: GraphQlReqVars{
			PageNo:     pageInfo.Page,
			PerPage:    pageInfo.PerPage,
			Season:     seasonInfo.Season,
			SeasonYear: seasonInfo.Year,
			IsAdult:    seasonInfo.IsAdult,
		},
	}

	seasonPageJsonB, err := json.Marshal(seasonPageGQLBody)
	if err != nil {
		maim("creating json for request to anilist", err)
	}

	fmt.Println("AniList GraphQL Body:")
	fmt.Println(string(seasonPageJsonB))

	req, err := http.NewRequest("POST", "https://graphql.anilist.co", bytes.NewBuffer(seasonPageJsonB))
	if err != nil {
		maim("creating http request", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	httpClient := &http.Client{}

	resp, err := httpClient.Do(req)
	if err != nil {
		maim("making post request to anilist", err)
	}

	httpRequests++

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)

	httpBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		maim("reading response from anilist", err)
	}

	fmt.Println("AniList response", string(httpBody))

	var aniListData AniListData

	if resp.StatusCode == http.StatusOK && resp.StatusCode < 300 {
		err := json.Unmarshal(httpBody, &aniListData)

		if err != nil {
			maim("parsing json from anilist", err)
		}
	}

	return AniListResultsPage{Page: aniListData}
}

func getAnimeSeason(seasonInfo *AniListSeason) {
	pageInfo := AniListSeasonPage{
		Page:    1,
		PerPage: 50,
	}

	results := make([]AniListResultsPage, 0)

	page := getAnimeSeasonByPage(seasonInfo, pageInfo)
	results = append(results, page)

	lastPage := page.Page.Data.Page.PageInfo.LastPage

	for lastPage > pageInfo.Page {
		pageInfo.Page = pageInfo.Page + 1

		page = getAnimeSeasonByPage(seasonInfo, pageInfo)
		results = append(results, page)
	}

	seasonInfo.Results = results
}

func start() {
	encodedJson, err := ioutil.ReadFile("config/config.json")
	if err != nil {
		kill("could not load config file", err)
	}

	config := AppConfig{}

	err = json.Unmarshal(encodedJson, &config)
	if err != nil {
		kill("could not parse json in config file", err)
	}

	httpRequests = 0
	fmt.Println("Beginning to scrape media at", time.Now().Format(time.RFC1123Z))

	years := make([]int, 0)
	currentTime := time.Now()
	currentYear := currentTime.Year()
	//currentYear := 1970
	years = append(years, currentYear-3, currentYear-2, currentYear-1, currentYear, currentYear+1)

	seasons := make([]string, 0)
	seasons = append(seasons, "SPRING", "SUMMER", "FALL", "WINTER")

	fetchedAnimeSeasons := make([]AniListSeason, 0)

	for i := range years {
		year := years[i]

		for s := range seasons {
			season := seasons[s]

			fmt.Println("getting", season, year)

			aniListSeason := AniListSeason{
				Season:  season,
				Year:    year,
				IsAdult: false,
			}

			getAnimeSeason(&aniListSeason)
			fetchedAnimeSeasons = append(fetchedAnimeSeasons, aniListSeason)
		}
	}

	fmt.Println("Made", httpRequests, "http requests fetching anime seasons from AniList")

	dbParams := make(map[string]string)
	dbParams["charset"] = "utf8mb4"
	dbParams["interpolateParams"] = "true"

	dbConfig := mysql.Config{
		User:   config.Db.User,
		Passwd: config.Db.Password,
		Net:    "tcp",
		Addr:   config.Db.Server,
		DBName: config.Db.DbName,
		Params: dbParams,
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		kill("could not open mysql connection", err)
	}

	defer func(db *sql.DB) {
		fmt.Println("Closing database connection at", time.Now().Format(time.RFC1123Z))
		err := db.Close()
		if err != nil {
			kill("could not close mysql connection", err)
		}
	}(db)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from database server being down", r)
		}
	}()

	err = db.Ping()
	if err != nil {
		kill("could not ping mysql server", err)
	}

	for j := range fetchedAnimeSeasons {
		animeSeason := fetchedAnimeSeasons[j]

		for k := range animeSeason.Results {
			animeSeasonPage := animeSeason.Results[k]
			seasonListMedia := animeSeasonPage.Page.Data.Page.Media

			for l := range seasonListMedia {
				mediaItem := seasonListMedia[l]
				pkMediaId := updateMedia(db, mediaItem)
				updateMediaTitles(db, pkMediaId, mediaItem)
				updateMediaSynonyms(db, pkMediaId, mediaItem)
				updateMediaGenres(db, pkMediaId, mediaItem)
			}
		}
	}
}

func main() {
	fmt.Println(
		"Started media scraper, waiting 2 minutes for other services to start... Began waiting at",
		time.Now().Format(time.RFC1123Z),
	)

	time.Sleep(2 * time.Minute)

	start()

	interval := 12 * time.Hour
	fmt.Println("Starting ticker to scrape media every", interval)

	ticker := time.NewTicker(interval)

	for _ = range ticker.C {
		start()
	}

	// Run application indefinitely
	select {}
}
