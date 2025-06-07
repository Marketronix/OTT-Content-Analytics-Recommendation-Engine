<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>OTT Analytics Project — IMDb Data Catalog</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 20px;
            padding: 20px;
            max-width: 1000px;
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
            margin-top: 10px;
        }
        th, td {
            border: 1px solid #bdc3c7;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color:rgb(195, 100, 28);
        }
        code {
            background-color: #f9f9f9;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: monospace;
        }
        pre {
            background-color:rgb(181, 66, 66);
            padding: 10px;
            border: 1px solid #ddd;
            overflow: auto;
        }
    </style>
</head>
<body>

<h1>📚 OTT Analytics Project — IMDb Data Catalog</h1>

<h2>Overview</h2>
<p>This document catalogs the IMDb datasets used in the OTT Analytics & Recommendation Engine project.</p>
<p>It records:</p>
<ul>
    <li>Dataset names</li>
    <li>Source schemas</li>
    <li>Update frequency</li>
    <li>Primary/foreign keys</li>
    <li>Notes on relationships</li>
</ul>

<h2>Datasets Overview</h2>
<table>
    <thead>
    <tr>
        <th>Dataset Name</th>
        <th>Source File</th>
        <th>Primary Key</th>
        <th>Notes</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>Title Akas</td>
        <td>title.akas.tsv.gz</td>
        <td>titleId + ordering</td>
        <td>Alternative titles per region</td>
    </tr>
    <tr>
        <td>Title Basics</td>
        <td>title.basics.tsv.gz</td>
        <td>tconst</td>
        <td>Core metadata about titles</td>
    </tr>
    <tr>
        <td>Title Crew</td>
        <td>title.crew.tsv.gz</td>
        <td>tconst</td>
        <td>Directors and writers mapping</td>
    </tr>
    <tr>
        <td>Title Episode</td>
        <td>title.episode.tsv.gz</td>
        <td>tconst</td>
        <td>Episode details; parent link</td>
    </tr>
    <tr>
        <td>Title Principals</td>
        <td>title.principals.tsv.gz</td>
        <td>tconst + ordering</td>
        <td>Key cast/crew info</td>
    </tr>
    <tr>
        <td>Title Ratings</td>
        <td>title.ratings.tsv.gz</td>
        <td>tconst</td>
        <td>Ratings and vote counts</td>
    </tr>
    <tr>
        <td>Name Basics</td>
        <td>name.basics.tsv.gz</td>
        <td>nconst</td>
        <td>Person info (actors, directors, etc.)</td>
    </tr>
    </tbody>
</table>

<h2>Dataset Details</h2>

<h3>📄 1. Title Akas</h3>
<p><strong>Primary Key:</strong> titleId + ordering<br>
<strong>Foreign Key:</strong> titleId → title.basics.tconst<br>
<strong>Update Frequency:</strong> Monthly (IMDb dumps)</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>titleId</td><td>STRING</td><td>Alphanumeric identifier (foreign key → title.basics.tconst)</td></tr>
    <tr><td>ordering</td><td>INTEGER</td><td>Unique row number per titleId</td></tr>
    <tr><td>title</td><td>STRING</td><td>Localized title</td></tr>
    <tr><td>region</td><td>STRING</td><td>Region</td></tr>
    <tr><td>language</td><td>STRING</td><td>Language</td></tr>
    <tr><td>types</td><td>ARRAY&lt;STRING&gt;</td><td>Title types</td></tr>
    <tr><td>attributes</td><td>ARRAY&lt;STRING&gt;</td><td>Title attributes</td></tr>
    <tr><td>isOriginalTitle</td><td>BOOLEAN</td><td>Original title flag</td></tr>
    </tbody>
</table>

<h3>📄 2. Title Basics</h3>
<p><strong>Primary Key:</strong> tconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>tconst</td><td>STRING</td><td>Title ID (Primary Key)</td></tr>
    <tr><td>titleType</td><td>STRING</td><td>Title type</td></tr>
    <tr><td>primaryTitle</td><td>STRING</td><td>Main title</td></tr>
    <tr><td>originalTitle</td><td>STRING</td><td>Original language title</td></tr>
    <tr><td>isAdult</td><td>BOOLEAN</td><td>Adult content flag</td></tr>
    <tr><td>startYear</td><td>INTEGER</td><td>Start year</td></tr>
    <tr><td>endYear</td><td>INTEGER</td><td>End year (TV Series)</td></tr>
    <tr><td>runtimeMinutes</td><td>INTEGER</td><td>Runtime in minutes</td></tr>
    <tr><td>genres</td><td>ARRAY&lt;STRING&gt;</td><td>List of genres</td></tr>
    </tbody>
</table>

<h3>📄 3. Title Crew</h3>
<p><strong>Primary Key:</strong> tconst<br>
<strong>Foreign Keys:</strong> tconst → title.basics.tconst; directors/writers → name.basics.nconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>tconst</td><td>STRING</td><td>Title ID (Primary Key)</td></tr>
    <tr><td>directors</td><td>ARRAY&lt;STRING&gt;</td><td>List of director nconst IDs</td></tr>
    <tr><td>writers</td><td>ARRAY&lt;STRING&gt;</td><td>List of writer nconst IDs</td></tr>
    </tbody>
</table>

<h3>📄 4. Title Episode</h3>
<p><strong>Primary Key:</strong> tconst<br>
<strong>Foreign Key:</strong> parentTconst → title.basics.tconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>tconst</td><td>STRING</td><td>Episode ID (Primary Key)</td></tr>
    <tr><td>parentTconst</td><td>STRING</td><td>Parent TV series ID</td></tr>
    <tr><td>seasonNumber</td><td>INTEGER</td><td>Season number</td></tr>
    <tr><td>episodeNumber</td><td>INTEGER</td><td>Episode number</td></tr>
    </tbody>
</table>

<h3>📄 5. Title Principals</h3>
<p><strong>Primary Key:</strong> tconst + ordering<br>
<strong>Foreign Keys:</strong> tconst → title.basics.tconst; nconst → name.basics.nconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>tconst</td><td>STRING</td><td>Title ID</td></tr>
    <tr><td>ordering</td><td>INTEGER</td><td>Row number per titleId</td></tr>
    <tr><td>nconst</td><td>STRING</td><td>Person ID</td></tr>
    <tr><td>category</td><td>STRING</td><td>Job category</td></tr>
    <tr><td>job</td><td>STRING</td><td>Specific job title</td></tr>
    <tr><td>characters</td><td>STRING</td><td>Character names</td></tr>
    </tbody>
</table>

<h3>📄 6. Title Ratings</h3>
<p><strong>Primary Key:</strong> tconst<br>
<strong>Foreign Key:</strong> tconst → title.basics.tconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>tconst</td><td>STRING</td><td>Title ID</td></tr>
    <tr><td>averageRating</td><td>FLOAT</td><td>Weighted average rating</td></tr>
    <tr><td>numVotes</td><td>INTEGER</td><td>Number of votes received</td></tr>
    </tbody>
</table>

<h3>📄 7. Name Basics</h3>
<p><strong>Primary Key:</strong> nconst<br>
<strong>Foreign Keys:</strong> knownForTitles → title.basics.tconst<br>
<strong>Update Frequency:</strong> Monthly</p>

<table>
    <thead>
    <tr>
        <th>Column Name</th>
        <th>Type</th>
        <th>Description</th>
    </tr>
    </thead>
    <tbody>
    <tr><td>nconst</td><td>STRING</td><td>Person ID</td></tr>
    <tr><td>primaryName</td><td>STRING</td><td>Person's name</td></tr>
    <tr><td>birthYear</td><td>INTEGER</td><td>Birth year</td></tr>
    <tr><td>deathYear</td><td>INTEGER</td><td>Death year (if applicable)</td></tr>
    <tr><td>primaryProfession</td><td>ARRAY&lt;STRING&gt;</td><td>Top 3 professions</td></tr>
    <tr><td>knownForTitles</td><td>ARRAY&lt;STRING&gt;</td><td>List of known titles</td></tr>
    </tbody>
</table>

<h2>Entity Relationships Diagram (Simplified)</h2>
<pre>
name.basics.nconst
        ↑
title.crew.directors, title.crew.writers
title.principals.nconst
        ↑
        |
        tconst → title.basics.tconst ← title.akas.titleId
          ↑                      ↑
  title.episode.tconst    title.ratings.tconst
  title.episode.parentTconst
</pre>

<h2>Notes</h2>
<ul>
    <li>All source data comes from <strong>IMDb official dataset dumps</strong> — publicly available, refreshed monthly.</li>
    <li>Primary keys are consistent across datasets (<code>tconst</code> for titles, <code>nconst</code> for persons).</li>
    <li>There is rich cross-linking for building <strong>dimensional models</strong>.</li>
</ul>

</body>
</html>
