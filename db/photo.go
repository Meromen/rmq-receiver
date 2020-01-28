package db

type Photo struct {
	Id             string
	Url            string
	IdempotencyKey string
	FilePath       string
}
