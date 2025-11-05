// NOTE: The following script syntax is valid for database version 5.0 and above.

:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'netflix_info.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `netflixLink_NetflixContent_uniq` IF NOT EXISTS
FOR (n: `NetflixContent`)
REQUIRE (n.`netflixLink`) IS UNIQUE;
CREATE CONSTRAINT `Genre_Genre_uniq` IF NOT EXISTS
FOR (n: `Genre`)
REQUIRE (n.`Genre`) IS UNIQUE;
CREATE CONSTRAINT `Tags_Tags_uniq` IF NOT EXISTS
FOR (n: `Tags`)
REQUIRE (n.`Tags`) IS UNIQUE;
CREATE CONSTRAINT `Languages_Languages_uniq` IF NOT EXISTS
FOR (n: `Languages`)
REQUIRE (n.`Languages`) IS UNIQUE;
CREATE CONSTRAINT `Series_or Movie_Series_or Movie_uniq` IF NOT EXISTS
FOR (n: `Series or Movie`)
REQUIRE (n.`Series or Movie`) IS UNIQUE;
CREATE CONSTRAINT `Hidden_Gem Score_Hidden_Gem Score_uniq` IF NOT EXISTS
FOR (n: `Hidden Gem Score`)
REQUIRE (n.`Hidden Gem Score`) IS UNIQUE;
CREATE CONSTRAINT `Country_Availability_Country_Availability_uniq` IF NOT EXISTS
FOR (n: `Country Availability`)
REQUIRE (n.`Country Availability`) IS UNIQUE;
CREATE CONSTRAINT `Runtime_Runtime_uniq` IF NOT EXISTS
FOR (n: `Runtime`)
REQUIRE (n.`Runtime`) IS UNIQUE;
CREATE CONSTRAINT `Director_Director_uniq` IF NOT EXISTS
FOR (n: `Director`)
REQUIRE (n.`Director`) IS UNIQUE;
CREATE CONSTRAINT `Writer_Writer_uniq` IF NOT EXISTS
FOR (n: `Writer`)
REQUIRE (n.`Writer`) IS UNIQUE;
CREATE CONSTRAINT `Actors_Actors_uniq` IF NOT EXISTS
FOR (n: `Actors`)
REQUIRE (n.`Actors`) IS UNIQUE;
CREATE CONSTRAINT `View_Rating_View_Rating_uniq` IF NOT EXISTS
FOR (n: `View Rating`)
REQUIRE (n.`View Rating`) IS UNIQUE;
CREATE CONSTRAINT `IMDb_Score_IMDb_Score_uniq` IF NOT EXISTS
FOR (n: `IMDb Score`)
REQUIRE (n.`IMDb Score`) IS UNIQUE;
CREATE CONSTRAINT `Rotten_Tomatoes Score_Rotten_Tomatoes Score_uniq` IF NOT EXISTS
FOR (n: `Rotten Tomatoes Score`)
REQUIRE (n.`Rotten Tomatoes Score`) IS UNIQUE;
CREATE CONSTRAINT `Metacritic_Score_Metacritic_Score_uniq` IF NOT EXISTS
FOR (n: `Metacritic Score`)
REQUIRE (n.`Metacritic Score`) IS UNIQUE;
CREATE CONSTRAINT `Awards_Received_Awards_Received_uniq` IF NOT EXISTS
FOR (n: `Awards Received`)
REQUIRE (n.`Awards Received`) IS UNIQUE;
CREATE CONSTRAINT `Awards_Nominated For_Awards_Nominated For_uniq` IF NOT EXISTS
FOR (n: `Awards Nominated For`)
REQUIRE (n.`Awards Nominated For`) IS UNIQUE;
CREATE CONSTRAINT `Boxoffice__Boxoffice_uniq` IF NOT EXISTS
FOR (n: ` Boxoffice`)
REQUIRE (n.`Boxoffice`) IS UNIQUE;
CREATE CONSTRAINT `Release_Date_Release_Date_uniq` IF NOT EXISTS
FOR (n: `Release Date`)
REQUIRE (n.`Release Date`) IS UNIQUE;
CREATE CONSTRAINT `Netflix_Release Date_Netflix_Release Date_uniq` IF NOT EXISTS
FOR (n: `Netflix Release Date`)
REQUIRE (n.`Netflix Release Date`) IS UNIQUE;
CREATE CONSTRAINT `Production_House_Production_House_uniq` IF NOT EXISTS
FOR (n: `Production House`)
REQUIRE (n.`Production House`) IS UNIQUE;
CREATE CONSTRAINT `Netflix_Link_Netflix_Link_uniq` IF NOT EXISTS
FOR (n: `Netflix Link`)
REQUIRE (n.`Netflix Link`) IS UNIQUE;
CREATE CONSTRAINT `IMDb_Votes_IMDb_Link_uniq` IF NOT EXISTS
FOR (n: `IMDb Link`)
REQUIRE (n.`IMDb Votes`) IS UNIQUE;
CREATE CONSTRAINT `Image_Image_uniq` IF NOT EXISTS
FOR (n: `Image`)
REQUIRE (n.`Image`) IS UNIQUE;
CREATE CONSTRAINT `Poster_Poster_uniq` IF NOT EXISTS
FOR (n: `Poster`)
REQUIRE (n.`Poster`) IS UNIQUE;
CREATE CONSTRAINT `TMDb_Trailer_TMDb_Trailer_uniq` IF NOT EXISTS
FOR (n: `TMDb Trailer`)
REQUIRE (n.`TMDb Trailer`) IS UNIQUE;
CREATE CONSTRAINT `Trailer_Site_Trailer_Site_uniq` IF NOT EXISTS
FOR (n: `Trailer Site`)
REQUIRE (n.`Trailer Site`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Netflix Link` IN $idsToSkip AND NOT row.`Netflix Link` IS NULL
AND row.`Title` = ''
CALL (row) {
  MERGE (n: `NetflixContent` { `netflixLink`: row.`Netflix Link` })
  SET n.`netflixLink` = row.`Netflix Link`
  SET n.`title` = row.`Title`
  SET n.`genre` = row.`Genre`
  SET n.`tag` = row.`Tags`
  SET n.`language` = row.`Languages`
  SET n.`contentType` = row.`Series or Movie`
  SET n.`hiddenGemScore` = toFloat(trim(row.`Hidden Gem Score`))
  SET n.`countryAvailability` = row.`Country Availability`
  SET n.`runtime` = row.`Runtime`
  SET n.`director` = row.`Director`
  SET n.`writer` = row.`Writer`
  SET n.`actor` = row.`Actors`
  SET n.`viewRating` = row.`View Rating`
  SET n.`imdbScore` = toFloat(trim(row.`IMDb Score`))
  SET n.`rottenTomatoesScore` = toFloat(trim(row.`Rotten Tomatoes Score`))
  SET n.`metacriticScore` = toFloat(trim(row.`Metacritic Score`))
  SET n.`awardReceived` = toFloat(trim(row.`Awards Received`))
  SET n.`awardNominatedFor` = toFloat(trim(row.`Awards Nominated For`))
  SET n.`boxOffice` = row.`Boxoffice`
  SET n.`releaseDate` = row.`Release Date`
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`netflixReleaseDate` = datetime(row.`Netflix Release Date`)
  SET n.`productionHouse` = row.`Production House`
  SET n.`imdbLink` = row.`IMDb Link`
  SET n.`summary` = row.`Summary`
  SET n.`imdbVote` = toFloat(trim(row.`IMDb Votes`))
  SET n.`image` = row.`Image`
  SET n.`poster` = row.`Poster`
  SET n.`tmdbTrailer` = row.`TMDb Trailer`
  SET n.`trailerSite` = row.`Trailer Site`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Genre` IN $idsToSkip AND NOT row.`Genre` IS NULL
AND row.`Genre` = ''
CALL (row) {
  MERGE (n: `Genre` { `Genre`: row.`Genre` })
  SET n.`Genre` = row.`Genre`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Tags` IN $idsToSkip AND NOT row.`Tags` IS NULL
CALL (row) {
  MERGE (n: `Tags` { `Tags`: row.`Tags` })
  SET n.`Tags` = row.`Tags`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Languages` IN $idsToSkip AND NOT row.`Languages` IS NULL
CALL (row) {
  MERGE (n: `Languages` { `Languages`: row.`Languages` })
  SET n.`Languages` = row.`Languages`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Series or Movie` IN $idsToSkip AND NOT row.`Series or Movie` IS NULL
CALL (row) {
  MERGE (n: `Series or Movie` { `Series or Movie`: row.`Series or Movie` })
  SET n.`Series or Movie` = row.`Series or Movie`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Hidden Gem Score` IN $idsToSkip AND NOT toFloat(trim(row.`Hidden Gem Score`)) IS NULL
CALL (row) {
  MERGE (n: `Hidden Gem Score` { `Hidden Gem Score`: toFloat(trim(row.`Hidden Gem Score`)) })
  SET n.`Hidden Gem Score` = toFloat(trim(row.`Hidden Gem Score`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Country Availability` IN $idsToSkip AND NOT row.`Country Availability` IS NULL
CALL (row) {
  MERGE (n: `Country Availability` { `Country Availability`: row.`Country Availability` })
  SET n.`Country Availability` = row.`Country Availability`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Runtime` IN $idsToSkip AND NOT row.`Runtime` IS NULL
CALL (row) {
  MERGE (n: `Runtime` { `Runtime`: row.`Runtime` })
  SET n.`Runtime` = row.`Runtime`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Director` IN $idsToSkip AND NOT row.`Director` IS NULL
CALL (row) {
  MERGE (n: `Director` { `Director`: row.`Director` })
  SET n.`Director` = row.`Director`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Writer` IN $idsToSkip AND NOT row.`Writer` IS NULL
CALL (row) {
  MERGE (n: `Writer` { `Writer`: row.`Writer` })
  SET n.`Writer` = row.`Writer`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Actors` IN $idsToSkip AND NOT row.`Actors` IS NULL
CALL (row) {
  MERGE (n: `Actors` { `Actors`: row.`Actors` })
  SET n.`Actors` = row.`Actors`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`View Rating` IN $idsToSkip AND NOT row.`View Rating` IS NULL
CALL (row) {
  MERGE (n: `View Rating` { `View Rating`: row.`View Rating` })
  SET n.`View Rating` = row.`View Rating`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`IMDb Score` IN $idsToSkip AND NOT toFloat(trim(row.`IMDb Score`)) IS NULL
CALL (row) {
  MERGE (n: `IMDb Score` { `IMDb Score`: toFloat(trim(row.`IMDb Score`)) })
  SET n.`IMDb Score` = toFloat(trim(row.`IMDb Score`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Rotten Tomatoes Score` IN $idsToSkip AND NOT toFloat(trim(row.`Rotten Tomatoes Score`)) IS NULL
CALL (row) {
  MERGE (n: `Rotten Tomatoes Score` { `Rotten Tomatoes Score`: toFloat(trim(row.`Rotten Tomatoes Score`)) })
  SET n.`Rotten Tomatoes Score` = toFloat(trim(row.`Rotten Tomatoes Score`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Metacritic Score` IN $idsToSkip AND NOT toFloat(trim(row.`Metacritic Score`)) IS NULL
CALL (row) {
  MERGE (n: `Metacritic Score` { `Metacritic Score`: toFloat(trim(row.`Metacritic Score`)) })
  SET n.`Metacritic Score` = toFloat(trim(row.`Metacritic Score`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Awards Received` IN $idsToSkip AND NOT toFloat(trim(row.`Awards Received`)) IS NULL
CALL (row) {
  MERGE (n: `Awards Received` { `Awards Received`: toFloat(trim(row.`Awards Received`)) })
  SET n.`Awards Received` = toFloat(trim(row.`Awards Received`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Awards Nominated For` IN $idsToSkip AND NOT toFloat(trim(row.`Awards Nominated For`)) IS NULL
CALL (row) {
  MERGE (n: `Awards Nominated For` { `Awards Nominated For`: toFloat(trim(row.`Awards Nominated For`)) })
  SET n.`Awards Nominated For` = toFloat(trim(row.`Awards Nominated For`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Boxoffice` IN $idsToSkip AND NOT row.`Boxoffice` IS NULL
CALL (row) {
  MERGE (n: ` Boxoffice` { `Boxoffice`: row.`Boxoffice` })
  SET n.`Boxoffice` = row.`Boxoffice`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Release Date` IN $idsToSkip AND NOT row.`Release Date` IS NULL
CALL (row) {
  MERGE (n: `Release Date` { `Release Date`: row.`Release Date` })
  SET n.`Release Date` = row.`Release Date`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Netflix Release Date` IN $idsToSkip AND NOT datetime(row.`Netflix Release Date`) IS NULL
CALL (row) {
  MERGE (n: `Netflix Release Date` { `Netflix Release Date`: datetime(row.`Netflix Release Date`) })
  // Your script contains the datetime datatype. Our app attempts to convert dates to ISO 8601 date format before passing them to the Cypher function.
  // This conversion cannot be done in a Cypher script load. Please ensure that your CSV file columns are in ISO 8601 date format to ensure equivalent loads.
  SET n.`Netflix Release Date` = datetime(row.`Netflix Release Date`)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Production House` IN $idsToSkip AND NOT row.`Production House` IS NULL
CALL (row) {
  MERGE (n: `Production House` { `Production House`: row.`Production House` })
  SET n.`Production House` = row.`Production House`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Netflix Link` IN $idsToSkip AND NOT row.`Netflix Link` IS NULL
CALL (row) {
  MERGE (n: `Netflix Link` { `Netflix Link`: row.`Netflix Link` })
  SET n.`Netflix Link` = row.`Netflix Link`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`IMDb Votes` IN $idsToSkip AND NOT toFloat(trim(row.`IMDb Votes`)) IS NULL
CALL (row) {
  MERGE (n: `IMDb Link` { `IMDb Votes`: toFloat(trim(row.`IMDb Votes`)) })
  SET n.`IMDb Votes` = toFloat(trim(row.`IMDb Votes`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Image` IN $idsToSkip AND NOT row.`Image` IS NULL
CALL (row) {
  MERGE (n: `Image` { `Image`: row.`Image` })
  SET n.`Image` = row.`Image`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Poster` IN $idsToSkip AND NOT row.`Poster` IS NULL
CALL (row) {
  MERGE (n: `Poster` { `Poster`: row.`Poster` })
  SET n.`Poster` = row.`Poster`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`TMDb Trailer` IN $idsToSkip AND NOT row.`TMDb Trailer` IS NULL
CALL (row) {
  MERGE (n: `TMDb Trailer` { `TMDb Trailer`: row.`TMDb Trailer` })
  SET n.`TMDb Trailer` = row.`TMDb Trailer`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Trailer Site` IN $idsToSkip AND NOT row.`Trailer Site` IS NULL
CALL (row) {
  MERGE (n: `Trailer Site` { `Trailer Site`: row.`Trailer Site` })
  SET n.`Trailer Site` = row.`Trailer Site`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `NetflixContent` { `netflixLink`: row.`Title` })
  MATCH (target: `Genre` { `Genre`: row.`Genre` })
  MERGE (source)-[r: `Lets Fight Ghost`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Genre` { `Genre`: row.`Genre` })
  MATCH (target: `Tags` { `Tags`: row.`Tags` })
  MERGE (source)-[r: `Crime, Drama`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Tags` { `Tags`: row.`Tags` })
  MATCH (target: `Languages` { `Languages`: row.`Languages` })
  MERGE (source)-[r: `Comedy Program`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Languages` { `Languages`: row.`Languages` })
  MATCH (target: `Series or Movie` { `Series or Movie`: row.`Series or Movie` })
  MERGE (source)-[r: `Swedish, Spanish`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Series or Movie` { `Series or Movie`: row.`Series or Movie` })
  MATCH (target: `Hidden Gem Score` { `Hidden Gem Score`: toFloat(trim(row.`Hidden Gem Score`)) })
  MERGE (source)-[r: `series`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Hidden Gem Score` { `Hidden Gem Score`: toFloat(trim(row.`Hidden Gem Score`)) })
  MATCH (target: `Country Availability` { `Country Availability`: row.`Country Availability` })
  MERGE (source)-[r: `4.3`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Country Availability` { `Country Availability`: row.`Country Availability` })
  MATCH (target: `Runtime` { `Runtime`: row.`Runtime` })
  MERGE (source)-[r: `Thailand`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Runtime` { `Runtime`: row.`Runtime` })
  MATCH (target: `Director` { `Director`: row.`Director` })
  MERGE (source)-[r: `< 30 minutes`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Director` { `Director`: row.`Director` })
  MATCH (target: `Writer` { `Writer`: row.`Writer` })
  MERGE (source)-[r: `Tomas Alfredson`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Writer` { `Writer`: row.`Writer` })
  MATCH (target: `Actors` { `Actors`: row.`Actors` })
  MERGE (source)-[r: `John Ajvide Lindqvist`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Actors` { `Actors`: row.`Actors` })
  MATCH (target: `View Rating` { `View Rating`: row.`View Rating` })
  MERGE (source)-[r: ` Kåre Hedebrant, Per R`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `View Rating` { `View Rating`: row.`View Rating` })
  MATCH (target: `IMDb Score` { `IMDb Score`: toFloat(trim(row.`IMDb Score`)) })
  MERGE (source)-[r: ` R`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `IMDb Score` { `IMDb Score`: toFloat(trim(row.`IMDb Score`)) })
  MATCH (target: `Rotten Tomatoes Score` { `Rotten Tomatoes Score`: toFloat(trim(row.`Rotten Tomatoes Score`)) })
  MERGE (source)-[r: `7.9`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Rotten Tomatoes Score` { `Rotten Tomatoes Score`: toFloat(trim(row.`Rotten Tomatoes Score`)) })
  MATCH (target: `Metacritic Score` { `Metacritic Score`: toFloat(trim(row.`Metacritic Score`)) })
  MERGE (source)-[r: `98.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Metacritic Score` { `Metacritic Score`: toFloat(trim(row.`Metacritic Score`)) })
  MATCH (target: `Awards Received` { `Awards Received`: toFloat(trim(row.`Awards Received`)) })
  MERGE (source)-[r: `82.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Awards Received` { `Awards Received`: toFloat(trim(row.`Awards Received`)) })
  MATCH (target: `Awards Nominated For` { `Awards Nominated For`: toFloat(trim(row.`Awards Nominated For`)) })
  MERGE (source)-[r: `74.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Awards Nominated For` { `Awards Nominated For`: toFloat(trim(row.`Awards Nominated For`)) })
  MATCH (target: ` Boxoffice` { `Boxoffice`: row.`Boxoffice` })
  MERGE (source)-[r: `57.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: ` Boxoffice` { `Boxoffice`: row.`Boxoffice` })
  MATCH (target: `Release Date` { `Release Date`: row.`Release Date` })
  MERGE (source)-[r: ` $2,122,065`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Release Date` { `Release Date`: row.`Release Date` })
  MATCH (target: `Netflix Release Date` { `Netflix Release Date`: datetime(row.`Netflix Release Date`) })
  MERGE (source)-[r: `12 Dec 2008`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Netflix Release Date` { `Netflix Release Date`: datetime(row.`Netflix Release Date`) })
  MATCH (target: `Production House` { `Production House`: row.`Production House` })
  MERGE (source)-[r: `2021-03-04`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Production House` { `Production House`: row.`Production House` })
  MATCH (target: `Netflix Link` { `Netflix Link`: row.`Netflix Link` })
  MERGE (source)-[r: `Canal+, Sandrew M`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Netflix Link` { `Netflix Link`: row.`Netflix Link` })
  MATCH (target: `IMDb Link` { `IMDb Votes`: toFloat(trim(row.`IMDb Votes`)) })
  MERGE (source)-[r: ` https://www.netflix.`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `IMDb Link` { `IMDb Votes`: toFloat(trim(row.`IMDb Votes`)) })
  MATCH (target: `Image` { `Image`: row.`Image` })
  MERGE (source)-[r: `205926.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Image` { `Image`: row.`Image` })
  MATCH (target: `Poster` { `Poster`: row.`Poster` })
  MERGE (source)-[r: `https://occ-0-4708-6`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `Poster` { `Poster`: row.`Poster` })
  MATCH (target: `TMDb Trailer` { `TMDb Trailer`: row.`TMDb Trailer` })
  MERGE (source)-[r: ` https://m.media`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL (row) {
  MATCH (source: `TMDb Trailer` { `TMDb Trailer`: row.`TMDb Trailer` })
  MATCH (target: `Trailer Site` { `Trailer Site`: row.`Trailer Site` })
  MERGE (source)-[r: `TMDb Trailer`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
