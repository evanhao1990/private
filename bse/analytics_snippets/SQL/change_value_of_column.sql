-- ----------------------------------------Change hre_flag to new cm1 logic-------------------------------------------------------------------------
UPDATE hre_training zt
SET zt.threshold = (
							SELECT new_ths.threshold
							FROM (
									SELECT 
									trn.DimStyleOptionID,
									CASE WHEN trn.price_group in ('0-10', '10-20', '20-30', '30-40', '40-50') THEN 0.742253 ELSE 0.79809 END as threshold
									FROM hre_training trn
						  		) new_ths
						  	WHERE new_ths.DimStyleOptionID = zt.DimStyleOptionID);

UPDATE hre_training zt
SET zt.hre_flag = (
							SELECT new_flag.hre_flag
							FROM (
									SELECT trn.DimStyleOptionID,
											 CASE WHEN trn.16w_rr > trn.threshold THEN 1 ELSE 0 END as hre_flag
									FROM hre_training trn
									) new_flag
							WHERE zt.DimStyleOptionID = new_flag.DimStyleOptionID)
				
						  