def save_image(film_id: str) -> None:
    from imdb import IMDb
    # create an instance of the IMDb class
    ia = IMDb()

    # get a movie
    movie = ia.get_movie(film_id)
    print(movie['full-size cover url'])


if __name__ == '__main__':
    save_image('0113277')
