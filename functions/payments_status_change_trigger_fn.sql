BEGIN
    NEW.updated_at := now();

    IF TG_OP = 'UPDATE'
       AND OLD.payment_status IS NOT DISTINCT FROM NEW.payment_status THEN
       RETURN NEW;
    END IF;

    PERFORM handle_payment_event(
        NEW.supabase_id,
        COALESCE(OLD.payment_status, 'NULL'),
        NEW.payment_status
    );

    RETURN NEW;
END;
