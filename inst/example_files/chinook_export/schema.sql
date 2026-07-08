


CREATE TABLE __SCHEMA__Artist(ArtistId INTEGER, "Name" VARCHAR(256), PRIMARY KEY(ArtistId));
CREATE TABLE __SCHEMA__Employee(EmployeeId INTEGER, LastName VARCHAR(256) NOT NULL, FirstName VARCHAR(256) NOT NULL, Title VARCHAR(256), ReportsTo INTEGER, BirthDate DATE, HireDate DATE, Address VARCHAR(256), City VARCHAR(256), State VARCHAR(256), Country VARCHAR(256), PostalCode VARCHAR(256), Phone VARCHAR(256), Fax VARCHAR(256), Email VARCHAR(256), PRIMARY KEY(EmployeeId));
CREATE TABLE __SCHEMA__Genre(GenreId INTEGER, "Name" VARCHAR(256), PRIMARY KEY(GenreId));
CREATE TABLE __SCHEMA__MediaType(MediaTypeId INTEGER, "Name" VARCHAR(256), PRIMARY KEY(MediaTypeId));
CREATE TABLE __SCHEMA__Playlist(PlaylistId INTEGER, "Name" VARCHAR(256), PRIMARY KEY(PlaylistId));
CREATE TABLE __SCHEMA__Album(AlbumId INTEGER, Title VARCHAR(256) NOT NULL, ArtistId INTEGER NOT NULL, PRIMARY KEY(AlbumId), FOREIGN KEY (ArtistId) REFERENCES __SCHEMA__Artist(ArtistId));
CREATE TABLE __SCHEMA__Customer(CustomerId INTEGER, FirstName VARCHAR(256) NOT NULL, LastName VARCHAR(256) NOT NULL, Company VARCHAR(256), Address VARCHAR(256), City VARCHAR(256), State VARCHAR(256), Country VARCHAR(256), PostalCode VARCHAR(256), Phone VARCHAR(256), Fax VARCHAR(256), Email VARCHAR(256) NOT NULL, SupportRepId INTEGER, PRIMARY KEY(CustomerId), FOREIGN KEY (SupportRepId) REFERENCES __SCHEMA__Employee(EmployeeId));
CREATE TABLE __SCHEMA__Invoice(InvoiceId INTEGER, CustomerId INTEGER NOT NULL, InvoiceDate DATE NOT NULL, BillingAddress VARCHAR(256), BillingCity VARCHAR(256), BillingState VARCHAR(256), BillingCountry VARCHAR(256), BillingPostalCode VARCHAR(256), Total DECIMAL(10,2) NOT NULL, PRIMARY KEY(InvoiceId), FOREIGN KEY (CustomerId) REFERENCES __SCHEMA__Customer(CustomerId));
CREATE TABLE __SCHEMA__Track(TrackId INTEGER, "Name" VARCHAR(256) NOT NULL, AlbumId INTEGER, MediaTypeId INTEGER NOT NULL, GenreId INTEGER, Composer VARCHAR(256), "Milliseconds" INTEGER NOT NULL, Bytes INTEGER, UnitPrice DECIMAL(10,2) NOT NULL, PRIMARY KEY(TrackId), FOREIGN KEY (AlbumId) REFERENCES __SCHEMA__Album(AlbumId), FOREIGN KEY (GenreId) REFERENCES __SCHEMA__Genre(GenreId), FOREIGN KEY (MediaTypeId) REFERENCES __SCHEMA__MediaType(MediaTypeId));
CREATE TABLE __SCHEMA__InvoiceLine(InvoiceLineId INTEGER, InvoiceId INTEGER NOT NULL, TrackId INTEGER NOT NULL, UnitPrice DECIMAL(10,2) NOT NULL, Quantity INTEGER NOT NULL, PRIMARY KEY(InvoiceLineId), FOREIGN KEY (InvoiceId) REFERENCES __SCHEMA__Invoice(InvoiceId), FOREIGN KEY (TrackId) REFERENCES __SCHEMA__Track(TrackId));
CREATE TABLE __SCHEMA__PlaylistTrack(PlaylistId INTEGER, TrackId INTEGER, PRIMARY KEY(PlaylistId, TrackId), FOREIGN KEY (PlaylistId) REFERENCES __SCHEMA__Playlist(PlaylistId), FOREIGN KEY (TrackId) REFERENCES __SCHEMA__Track(TrackId));


CREATE INDEX IFK_AlbumArtistId ON __SCHEMA__Album(ArtistId);
CREATE INDEX IFK_CustomerSupportRepId ON __SCHEMA__Customer(SupportRepId);
CREATE INDEX IFK_EmployeeReportsTo ON __SCHEMA__Employee(ReportsTo);
CREATE INDEX IFK_InvoiceCustomerId ON __SCHEMA__Invoice(CustomerId);
CREATE INDEX IFK_InvoiceLineInvoiceId ON __SCHEMA__InvoiceLine(InvoiceId);
CREATE INDEX IFK_InvoiceLineTrackId ON __SCHEMA__InvoiceLine(TrackId);
CREATE INDEX IFK_PlaylistTrackTrackId ON __SCHEMA__PlaylistTrack(TrackId);
CREATE INDEX IFK_TrackAlbumId ON __SCHEMA__Track(AlbumId);
CREATE INDEX IFK_TrackGenreId ON __SCHEMA__Track(GenreId);
CREATE INDEX IFK_TrackMediaTypeId ON __SCHEMA__Track(MediaTypeId);


