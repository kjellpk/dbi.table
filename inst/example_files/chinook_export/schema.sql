


CREATE TABLE Artist(ArtistId INTEGER, "Name" VARCHAR, PRIMARY KEY(ArtistId));
CREATE TABLE Employee(EmployeeId INTEGER, LastName VARCHAR NOT NULL, FirstName VARCHAR NOT NULL, Title VARCHAR, ReportsTo INTEGER, BirthDate TIMESTAMP, HireDate TIMESTAMP, Address VARCHAR, City VARCHAR, State VARCHAR, Country VARCHAR, PostalCode VARCHAR, Phone VARCHAR, Fax VARCHAR, Email VARCHAR, PRIMARY KEY(EmployeeId));
CREATE TABLE Genre(GenreId INTEGER, "Name" VARCHAR, PRIMARY KEY(GenreId));
CREATE TABLE MediaType(MediaTypeId INTEGER, "Name" VARCHAR, PRIMARY KEY(MediaTypeId));
CREATE TABLE Playlist(PlaylistId INTEGER, "Name" VARCHAR, PRIMARY KEY(PlaylistId));
CREATE TABLE Album(AlbumId INTEGER, Title VARCHAR NOT NULL, ArtistId INTEGER NOT NULL, PRIMARY KEY(AlbumId), FOREIGN KEY (ArtistId) REFERENCES Artist(ArtistId));
CREATE TABLE Customer(CustomerId INTEGER, FirstName VARCHAR NOT NULL, LastName VARCHAR NOT NULL, Company VARCHAR, Address VARCHAR, City VARCHAR, State VARCHAR, Country VARCHAR, PostalCode VARCHAR, Phone VARCHAR, Fax VARCHAR, Email VARCHAR NOT NULL, SupportRepId INTEGER, PRIMARY KEY(CustomerId), FOREIGN KEY (SupportRepId) REFERENCES Employee(EmployeeId));
CREATE TABLE Invoice(InvoiceId INTEGER, CustomerId INTEGER NOT NULL, InvoiceDate TIMESTAMP NOT NULL, BillingAddress VARCHAR, BillingCity VARCHAR, BillingState VARCHAR, BillingCountry VARCHAR, BillingPostalCode VARCHAR, Total DECIMAL(10,2) NOT NULL, PRIMARY KEY(InvoiceId), FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId));
CREATE TABLE Track(TrackId INTEGER, "Name" VARCHAR NOT NULL, AlbumId INTEGER, MediaTypeId INTEGER NOT NULL, GenreId INTEGER, Composer VARCHAR, "Milliseconds" INTEGER NOT NULL, Bytes INTEGER, UnitPrice DECIMAL(10,2) NOT NULL, PRIMARY KEY(TrackId), FOREIGN KEY (AlbumId) REFERENCES Album(AlbumId), FOREIGN KEY (GenreId) REFERENCES Genre(GenreId), FOREIGN KEY (MediaTypeId) REFERENCES MediaType(MediaTypeId));
CREATE TABLE InvoiceLine(InvoiceLineId INTEGER, InvoiceId INTEGER NOT NULL, TrackId INTEGER NOT NULL, UnitPrice DECIMAL(10,2) NOT NULL, Quantity INTEGER NOT NULL, PRIMARY KEY(InvoiceLineId), FOREIGN KEY (InvoiceId) REFERENCES Invoice(InvoiceId), FOREIGN KEY (TrackId) REFERENCES Track(TrackId));
CREATE TABLE PlaylistTrack(PlaylistId INTEGER, TrackId INTEGER, PRIMARY KEY(PlaylistId, TrackId), FOREIGN KEY (PlaylistId) REFERENCES Playlist(PlaylistId), FOREIGN KEY (TrackId) REFERENCES Track(TrackId));


CREATE INDEX IFK_AlbumArtistId ON Album(ArtistId);
CREATE INDEX IFK_CustomerSupportRepId ON Customer(SupportRepId);
CREATE INDEX IFK_EmployeeReportsTo ON Employee(ReportsTo);
CREATE INDEX IFK_InvoiceCustomerId ON Invoice(CustomerId);
CREATE INDEX IFK_InvoiceLineInvoiceId ON InvoiceLine(InvoiceId);
CREATE INDEX IFK_InvoiceLineTrackId ON InvoiceLine(TrackId);
CREATE INDEX IFK_PlaylistTrackTrackId ON PlaylistTrack(TrackId);
CREATE INDEX IFK_TrackAlbumId ON Track(AlbumId);
CREATE INDEX IFK_TrackGenreId ON Track(GenreId);
CREATE INDEX IFK_TrackMediaTypeId ON Track(MediaTypeId);


