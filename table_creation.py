drop table metric;
drop table measure;

CREATE TABLE `measure`
(
	measure_id VARCHAR(36) PRIMARY KEY,
	device_id TEXT, 
	description TEXT, 
	lat DOUBLE, 
	lng DOUBLE, 
	event_timestamp TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE `metric`
(
	measure_id VARCHAR(36),
	metric_name TEXT,
	metric_value DOUBLE,
	FOREIGN KEY fk_measure(measure_id)
	REFERENCES measure(measure_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;


CASE 
	WHEN metric_name = 'temperature' THEN metric_value
    ELSE 0.0
END
