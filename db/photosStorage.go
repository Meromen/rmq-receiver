package db

import "database/sql"

const (
	createPhotosTableQuery = `
		CREATE TABLE IF NOT EXISTS photos (
			"id" TEXT PRIMARY KEY,
			"url" TEXT,
			"idempotency_key" TEXT
		);
	`
	dropPhotosTableQuery string = `
		DROP TABLE IF EXISTS photos;
	`

	insertPhotoQuery = `
		INSERT INTO photos
		VALUES($1, $2, $3);
	`

	checkExistQuery = `
		SELECT * FROM photos
		WHERE "id" = $1 AND "idempotencyKey" = $2;
	`
)

type PhotosStorage struct {
	conn *sql.DB
}

func NewPhotoStorage(conn *sql.DB) PhotosStorage {
	return PhotosStorage{conn: conn}
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

	_, err = bdTx.Exec(insertPhotoQuery, photo.Id, photo.Url, photo.IdempotencyKey)
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
	if err := row.Scan(&photo.Id, &photo.Url, &photo.IdempotencyKey); err != nil {
		return false
	}

	return true
}
