CREATE TABLE [dbo].[Firms] (
    [Id]               INT            IDENTITY (1, 1) NOT NULL,
    [sellerName]       NVARCHAR (MAX) NOT NULL,
    [sellerInn]        VARCHAR (15)   NULL,
    [buyerName]        NVARCHAR (MAX) NOT NULL,
    [buyerInn]         VARCHAR (20)   NULL,
    [woodVolumeBuyer]  VARCHAR (25)   NOT NULL,
    [woodVolumeSeller] VARCHAR (25)   NOT NULL,
    [dealDate]         DATE           NOT NULL,
    [dealNumber]       VARCHAR (40)   NOT NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC)
);