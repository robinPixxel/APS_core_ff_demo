class ImageTableEntry:
    ImageID:str
    SatID:str
    DueDate:str
    Priority:float
    Tilestrips:int
    Sensors:int
    Bands:int
    EmergencyFlag:int #1,0
    CaptureDate:str #2024-01-01 0:00:00
    assured_downlink_flag: int #1,0
    delivery_type: str # 'standard_delivery','expedited_delivery','super_expedited_delivery'

class StripOpportunityEntry:
    SatID:str
    OpportunityStartTime:str  #2024-01-01 0:00:00
    OpportunityEndTime:str    #2024-01-01 0:00:00
    StripID:str
    OffNadir:float
    SunInView:int # 1,0
    EarthInView:int # 1,0
    MoonInView: int # 1,0
    Tasking_type:str
    OrderValidityStart:str # #2024-01-01 0:00:00
    OrderValidityEnd:str #2024-01-01 0:00:00
    AoiID:str
    Eclipse:int # 1,0
    CloudCoverLimit: float
    CloudCover: float
    OffNadirLimit : float
    Priority : float
    OpportunityStartOffset : int
    OpportunityEndOffset : int
    bias_priority : float
    pass

class GroundstationPassesEntry:
    GsID:str
    AOS:str 
    LOS:str
    Eclipse:int # 1,0
    AOSOffset:int 
    LOSOffset:int
    SatID : str
    pass

class EclipseEventTableEntry:
		
    SatID:str		
    Start_time:str		
    end_time:str			
    eclipse:int # 1,0	

class ThemalPowerMemoryParameterEntry:		
    SatID:str			
    device:str	#camera detector/XBT/NCCM		
    initial_temp:float			
    temp_cap:float			
    Heat_Eqn:str			
    cool_Eqn:str

class MemoryDataEntry:			
    SatID:	str		
    memory_device:str#	NCCM/SSD		
    initial_memory:float		
    memory_cap:float

class MemoryTransferRateSatLevelEntry:			
    SatID:str		
    imaging_rate:float		
    readout_rate:float

class MemoryTransferRateGSLevelEntry:			
    GsID:str
    SatID:str			
    downlink_rate:float	

class PowerDataEntry:			
    SatID:str			
    initial_power:float			
    power_cap:float		

class PowerTransferDataEntry:
    SatID:str	
    operation:str#	imaging/downlinking/readout/idle		
    sunlit_power_generate_rate:float			
    eclipse_power_consumption_rate:float			
    sunlit_power_consume_rate:float	

class ImageCaptureResult:
    SatID:str
    start_time:str
    end_time:str
    StripID:str
    AoiID:str

class ReadoutScheduleResult:
    SatID:str
    start_time:str
    end_time:str
  
class DownlinkScheduleResult:
    SatID:str
    gsID:str
    start_time:str
    ImageID:str
    end_time:str
    TileStripNo_downLoad:str
    Total_No_tilesStrip:str
    bands:int

class config:
    pass


    						
                            