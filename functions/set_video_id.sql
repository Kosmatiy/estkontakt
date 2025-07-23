BEGIN
  IF NEW.video_url IS NOT NULL THEN
    -- вытаскиваем 11-символьный ID любой ссылки youtube.com / youtu.be
    NEW.video_id :=
      regexp_replace(
        NEW.video_url,
        '^.*(?:youtu\.be/|youtube\.com/(?:watch\?v=|embed/|v/))([A-Za-z0-9_-]{11}).*$',
        '\1'
      );
  ELSE
    NEW.video_id := NULL;
  END IF;

  RETURN NEW;
END;
