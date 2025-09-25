CREATE DATABASE OpinionDB
ON PRIMARY
(
    NAME = N'OpinionDB_Primary',
    FILENAME = 'C:\Users\PC\Desktop\Tareas ITLA\Electiva 1 - Big Data\Unidad3\SQLData\OpinionDB_Primary.mdf',
    SIZE = 10MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 5MB
),
FILEGROUP FG_2024
(
    NAME = 'OpinionDB_2024',
    FILENAME = 'C:\Users\PC\Desktop\Tareas ITLA\Electiva 1 - Big Data\Unidad3\SQLData\Opinion2024\OpinionDB_2024.ndf',
    SIZE = 10MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 5MB
),
FILEGROUP FG_2025
(
    NAME = 'OpinionDB_2025',
    FILENAME = 'C:\Users\PC\Desktop\Tareas ITLA\Electiva 1 - Big Data\Unidad3\SQLData\Opinion2025\OpinionDB_2025.ndf',
    SIZE = 10MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 5MB
),
FILEGROUP FG_2026
(
    NAME = 'OpinionDB_2026',
    FILENAME = 'C:\Users\PC\Desktop\Tareas ITLA\Electiva 1 - Big Data\Unidad3\SQLData\Opinion2026\OpinionDB_2026.ndf',
    SIZE = 10MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 5MB
)
LOG ON
(
    NAME = N'OpinionDB_Log',
    FILENAME = 'C:\Users\PC\Desktop\Tareas ITLA\Electiva 1 - Big Data\Unidad3\SQLData\Logs\OpinionDB.ldf',
    SIZE = 10MB,
    MAXSIZE = 2GB,
    FILEGROWTH = 5MB
);
GO

USE OpinionDB;
GO

CREATE TABLE Clientes (
    IdCliente INT PRIMARY KEY,
    Nombre NVARCHAR(100) NOT NULL,
    Email NVARCHAR(150) UNIQUE NOT NULL
);

CREATE TABLE Categorias (
    IdCategoria INT PRIMARY KEY IDENTITY(1,1),
    Nombre NVARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Productos (
    IdProducto INT PRIMARY KEY,
    Nombre NVARCHAR(100) NOT NULL,
    IdCategoria INT,
    CONSTRAINT FK_Productos_Categoria FOREIGN KEY (IdCategoria) REFERENCES Categorias(IdCategoria)
);

CREATE TABLE Clasificaciones (
    IdClasificacion INT PRIMARY KEY IDENTITY(1,1),
    Nombre NVARCHAR(50) UNIQUE NOT NULL
);
GO

CREATE TABLE RegistroCargas (
    IdCarga INT PRIMARY KEY IDENTITY(1,1),
    Nombre NVARCHAR(50) UNIQUE NOT NULL,
    FechaCarga DATETIME NOT NULL
);

CREATE TABLE Fuentes (
    IdFuente INT PRIMARY KEY IDENTITY(1,1),
    Nombre NVARCHAR(100) UNIQUE NOT NULL -- Ej: 'Instagram', 'Twitter', 'Facebook'
);
GO

CREATE PARTITION FUNCTION pf_FechaRango (DATE)
AS RANGE LEFT FOR VALUES ('2024-12-31', '2025-12-31');
GO

CREATE PARTITION SCHEME ps_FechaRango
AS PARTITION pf_FechaRango
TO (FG_2024, FG_2025, FG_2026);
GO

CREATE TABLE Comentarios (
    IdComment VARCHAR(10) NOT NULL,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    IdFuente INT NOT NULL,
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    PRIMARY KEY NONCLUSTERED (IdComment, Fecha),
    CONSTRAINT FK_Comentarios_Cliente FOREIGN KEY (IdCliente) REFERENCES Clientes(IdCliente),
    CONSTRAINT FK_Comentarios_Producto FOREIGN KEY (IdProducto) REFERENCES Productos(IdProducto),
    CONSTRAINT FK_Comentarios_Fuente FOREIGN KEY (IdFuente) REFERENCES Fuentes(IdFuente)
)
ON ps_FechaRango(Fecha);
GO

CREATE TABLE Encuestas (
    IdOpinion INT NOT NULL,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    IdCarga INT NOT NULL,
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    IdClasificacion INT,
    PuntajeSatisfaccion INT CHECK (PuntajeSatisfaccion BETWEEN 1 AND 5),
    PRIMARY KEY NONCLUSTERED (IdOpinion, Fecha),
    CONSTRAINT FK_Encuestas_Clasificacion FOREIGN KEY (IdClasificacion) REFERENCES Clasificaciones(IdClasificacion),
    CONSTRAINT FK_Encuestas_Cliente FOREIGN KEY (IdCliente) REFERENCES Clientes(IdCliente),
    CONSTRAINT FK_Encuestas_Producto FOREIGN KEY (IdProducto) REFERENCES Productos(IdProducto),
    CONSTRAINT FK_Encuestas_Carga FOREIGN KEY (IdCarga) REFERENCES RegistroCargas(IdCarga)
)
ON ps_FechaRango(Fecha);
GO

CREATE TABLE WebReviews (
    IdReview VARCHAR(10) NOT NULL,
    IdCliente INT NOT NULL,
    IdProducto INT NOT NULL,
    IdCarga INT NOT NULL,
    Fecha DATE NOT NULL,
    Comentario NVARCHAR(MAX),
    Rating INT CHECK (Rating BETWEEN 1 AND 5),
    PRIMARY KEY NONCLUSTERED (IdReview, Fecha),
    CONSTRAINT FK_WebReviews_Cliente FOREIGN KEY (IdCliente) REFERENCES Clientes(IdCliente),
    CONSTRAINT FK_WebReviews_Producto FOREIGN KEY (IdProducto) REFERENCES Productos(IdProducto),
    CONSTRAINT FK_WebReviews_Carga FOREIGN KEY (IdCarga) REFERENCES RegistroCargas(IdCarga)
)
ON ps_FechaRango(Fecha);
GO