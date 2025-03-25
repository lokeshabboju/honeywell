DELIMITER $$;

DROP PROCEDURE IF EXISTS TransferService();


CREATE PROCEDURE TransferService(
     p_source_aircraft_id INT,
     p_destination_aircraft_id INT,
     p_service_id INT
)

BEGIN
    DECLARE v_customer_id_source INT;
    DECLARE v_customer_id_dest INT;
    DECLARE v_status VARCHAR(20);
    DECLARE v_asset_id INT;
    DECLARE v_new_asset_id INT;
	
	
    DECLARE exit handler FOR SQLEXCEPTION
    BEGIN
        
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error in transferring service';
    END;

   
    START TRANSACTION;
    
    -- Validate source and destination aircraft exist
    SELECT customer_id INTO v_customer_id_source FROM Aircraft WHERE aircraft_id = p_source_aircraft_id;
	
    IF v_customer_id_source IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Source aircraft not found';
    END IF;
    
    SELECT customer_id INTO v_customer_id_dest FROM Aircraft WHERE aircraft_id = p_destination_aircraft_id;
	
    IF v_customer_id_dest IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Destination aircraft not found';
    END IF;
    
    -- Ensure aircraft belong to the same airline
    IF v_customer_id_source != v_customer_id_dest THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Aircraft must belong to the same airline';
    END IF;
    
    -- Validate service status
    SELECT status, asset_id INTO v_status, v_asset_id FROM Services WHERE service_id = p_service_id;
	
    IF v_status IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Service not found';
    END IF;
	
    IF v_status = 'In Progress' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Service transfer not allowed while In Progress';
    END IF;
    
    -- Check for compatible asset in destination aircraft
    SELECT asset_id INTO v_new_asset_id FROM Assets WHERE aircraft_id = p_destination_aircraft_id AND asset_type = 
        (SELECT asset_type FROM Assets WHERE asset_id = v_asset_id) LIMIT 1;
    
    -- Assign a new asset if not found
    IF v_new_asset_id IS NULL THEN
        INSERT INTO Assets (aircraft_id, asset_type, serial_number, installed_date)
        SELECT p_destination_aircraft_id, asset_type, CONCAT('NEW-', UUID()), NOW()
        FROM Assets WHERE asset_id = v_asset_id;
        
        SET v_new_asset_id = LAST_INSERT_ID();
    END IF;
    
    -- Update the service record
    UPDATE Services 
    SET aircraft_id = p_destination_aircraft_id, asset_id = v_new_asset_id, service_date = NOW()
    WHERE service_id = p_service_id;
    
    -- Insert audit log
    INSERT INTO Service_Transfer_Log (service_id, source_aircraft_id, destination_aircraft_id, transfer_date)
    VALUES (p_service_id, p_source_aircraft_id, p_destination_aircraft_id, NOW());
    
    
    COMMIT;
	
END $$;

DELIMITER ;

-- To fetch top 5 aircraft with the highest number of service transfers in the past year

SELECT destination_aircraft_id, COUNT(*) AS transfer_count
FROM Service_Transfer_Log
WHERE transfer_date >= NOW() - INTERVAL 1 YEAR
GROUP BY destination_aircraft_id
ORDER BY transfer_count DESC
LIMIT 5;

-- To fetch aircraft with multiple service transfers within the same 30-day period

SELECT destination_aircraft_id, COUNT(*) AS transfer_count, 
       DATE_FORMAT(transfer_date, '%Y-%m') AS transfer_month
FROM Service_Transfer_Log
GROUP BY destination_aircraft_id, transfer_month
HAVING transfer_count > 1;
