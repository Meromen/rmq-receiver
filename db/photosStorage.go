package db

import "database/sql"

const (
	createPhotosTableQuery = `
		CREATE TABLE IF NOT EXISTS photos (
			"id" TEXT PRIMARY KEY,
			"url" TEXT,
			"idempotency_key" TEXT,
			"file_path" TEXT
		);
	`
	dropPhotosTableQuery string = `
		DROP TABLE IF EXISTS photos;
	`

	insertPhotoQuery = `
		INSERT INTO photos
		VALUES($1, $2, $3, $4);
	`

	checkExistQuery = `
		SELECT * FROM photos
		WHERE "id" = $1 AND "idempotency_key" = $2;
	`
)

type PhotosStorage struct {
	conn *sql.DB
}

func NewPhotoStorage(conn *sql.DB) (PhotosStorage, error) {
	storage := PhotosStorage{conn: conn}
	err := storage.CreateTable()
	if err != nil {
		return PhotosStorage{}, err
	}
	return PhotosStorage{conn: conn}, nil
}

func (s *PhotosStorage) CreateTable() error {
	bdTx, err := s.conn.Begin()
	if err != nil {
		return err
	}

	_, err = bdTx.Exec(createPhotosTableQuery)
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *PhotosStorage) DropTable() error {
	bdTx, err := s.conn.Begin()
	if err != nil {
		return err
	}

	_, err = bdTx.Exec(dropPhotosTableQuery)
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *PhotosStorage) InsertPhoto(photo *Photo) error {
	bdTx, err := s.conn.Begin()
	if err != nil {
		return err
	}

	_, err = bdTx.Exec(insertPhotoQuery, photo.Id, photo.Url, photo.IdempotencyKey, photo.FilePath)
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *PhotosStorage) CheckExisting(id string, idempotencyKey string) bool {
	row := s.conn.QueryRow(checkExistQuery, id, idempotencyKey)

	photo := Photo{}
	if err := row.Scan(&photo.Id, &photo.Url, &photo.IdempotencyKey, &photo.FilePath); err != nil {
		return false
	}

	return true
}
