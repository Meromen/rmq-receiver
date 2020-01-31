package db

type Photo struct {
	Id             string
	Url            string
	IdempotencyKey string
	FilePath       string
}

func (p Photo) GetId() string {
	return p.Id
}
