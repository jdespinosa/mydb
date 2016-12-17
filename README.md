# mydb
This is a simple java sql database library. Basically, the database corresponds to a folder that contains "table" files. For simplicity,
all "table" files are plain text based files. Each table and even the whole database uses xml files to describe database-specific and 
table settings.

The goals of this library are as follows:
> be able to easily integrate it on any java project
> create classes that would correspond to a database entity
> thru these classes, be easily able to parse and "objectize" basic sql query elements

The limitations of this library:
> not suited for simultaneous access (e.i. two or more applications accessing the same database/tables)
> no server thus the rationale for the first reason
> limited sql queries only (SELECT, UPDATE, DELETE, CREATE)
> limited to only three field data types: VARCHAR, INT & DOUBLE
> no security features like password and username
