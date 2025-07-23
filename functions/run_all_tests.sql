BEGIN
    PERFORM test_log('INFO','run_all_tests','Начало run_all_tests...');
    PERFORM run_test_1();
    PERFORM run_test_2();
    PERFORM run_test_3();
    PERFORM run_test_4();
    PERFORM run_test_5();
    PERFORM run_test_6();
    PERFORM run_test_7();
    -- (TEST-8 в ТЗ не описан, возможно пропуск)
    PERFORM run_test_9();
    PERFORM run_test_10();
    PERFORM run_test_11();
    PERFORM run_test_12();
    PERFORM run_test_13();
    PERFORM run_test_14();
    PERFORM run_test_15();
    PERFORM test_log('INFO','run_all_tests','Все тесты (1..15) выполнены.');
END;
