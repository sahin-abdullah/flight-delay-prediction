import numpy as np

def check(df):
    # Check common columns first
    print('WeekDay column passed check' if all(df.WeekDay.notnull()) else '***WeekDay column has null entries***')
    print('IATA column passed check' if all(df.IATA.notnull()) else '***IATA column has null entries***')
    print('TailNum column passed check' if all(df.TailNum.notnull()) else '***TailNum column has null entries***')
    print('FlightNum column passed check' if all(df.FlightNum.notnull()) else '***FlightNum column has null entries***')
    
    print('OrgAirID column passed check' if all(df.OrgAirID.notnull()) else '***OrgAirID column has null entries***')
    print('OrgMarID column passed check' if all(df.OrgMarID.notnull()) else '***OrgMarID column has null entries***')
    print('DestAirID column passed check' if all(df.DestAirID.notnull()) else '***DestAirID column has null entries***')
    print('DestMarID column passed check' if all(df.DestMarID.notnull()) else '***DestMarID column has null entries***')
    
    print('ScDepTime column passed check' if all(df.ScDepTime.notnull()) else '***ScDepTime column has null entries***')
    print('***A flight(s) is/are scheduled later than ScArrTime***' if df.loc[df.ScDepTime > df.ScArrTime].shape[0] > 0 else 'No flight is scheduled later than ScArrTime')
    cond = df[df.DepTime.isnull()].index == df[df.Cncl==1].index
    print('DepTime column passed check (1/2)' if all(cond) else '***DepTime column has null entries other than cancelled flights***')
    cond = df.DepTime.equals(df.ScDepTime + df.DepDelay)
    print('DepTime column passed check (2/2)' if cond else '***DepTime column is not sum of ScDepTime and DepDelay***')
    cond = df.WhOff.equals(df.DepTime + df.TxO)
    print('WhOff column passed check' if cond else '***WhOff column is not sum of DepTime + TxO')
    cond = df[df.TxO.isnull()].index == df[df.Cncl==1].index
    print('TxO column passed check (1/2)' if all(cond) else '***TxO column has null entries other than cancelled flights***')
    cond = df.TxO.equals(df.WhOff - df.DepTime)
    print('TxO column passed check (2/2)' if cond else '***TxO column is not equal to WhOff-DepTime***')
    
    print('ScArrTime column passed check' if all(df.ScArrTime.notnull()) else '***ScArrTime column has null entries***')
    cond = df[df.ArrTime.isnull()].index == df[(df.Cncl==1) | (df.Div == 1)].index
    print('ArrTime column passed check (1/2)' if all(cond) else '***ArrTime column has null entries other than cancelled/diverted flights***')
    cond = df.ArrTime.equals(df.ScArrTime + df.ArrDelay)
    print('ArrTime column passed check (2/2)' if cond else '***ArrTime column is not sum of ScArrTime and ArrDelay***')
    cond = df.WhOn.equals(df.ArrTime - df.TxI)
    print('WhOn column passed check' if cond else '***WhOn column is not equal to ArrTime - TxI')
    cond = df[df.TxI.isnull()].index == df[(df.Cncl==1) | (df.Div==1)].index
    print('TxI column passed check (1/2)' if all(cond) else '***TxI column has null entries other than cancelled flights***')
    cond = df.TxI.equals(df.ArrTime - df.WhOn)
    print('TxI column passed check (2/2)' if cond else '***TxI column is not equal to ArrTime-WhOn***')
    
    filt = df.iloc[:, range(9,18)].isnull().sum(axis=1) == 8
    print('Cncl column passed check (1/2)' if all(df.loc[filt].index == df[df.Cncl==1].index) else '***Four columns(range(9,18)) need attention for cancelled flights***')
    filt = df.iloc[:, range(13,18)].isnull().sum(axis=1) == 4
    print('Div column passed check (1/2)' if all(df.loc[filt].index == df[(df.Div==1) | df.Cncl==1].index) else '***Eight columns(range(13,18)) need attention for diverted flights***')
    print('***Some flights are both diverted and cancelled***' if any(np.isin(df[df.Div==1].index.values, df[df.Cncl==1].index.values)) else 'Cncl/Div columns passed check (2/2)')
    print('CnclCd column passed check' if df[df.CnclCd != 0].index.equals(df[df.Cncl==1].index) else '***CnclCd column is not consistent with Cncl column***')
    
    print('ScElaTime column passed check (1/2)' if all(df.ScElaTime.notnull()) else '***ScElaTime column has null entries***')    
    cond = df.ScElaTime.equals(df.ScArrTime - df.ScDepTime - df.TimeZoneDiff)
    print('ScElaTime column passed check (2/2)' if cond else '***ScElaTime is not equal to ScArrTime - ScDepTime - TimeZoneDiff***')
    cond = df[df.AcElaTime.isnull()].index.equals(df[(df.Cncl==1) | (df.Div==1)].index)
    print('AcElaTime column passed check (1/2)' if cond else '***AcElaTime has null entries other than cancelled/diverted flights***')
    cond = df.AcElaTime.equals(df.ArrTime - df.DepTime - df.TimeZoneDiff)
    print('AcElaTime column passed check (2/2)' if cond else '***AcElaTime is not equal to ArrTime - DepTime - TimeZoneDiff***')
    
    cond = df[df.AirTime.isnull()].index.equals(df[(df.Cncl==1) | (df.Div==1)].index)
    print('AirTime column passed check (1/2)' if cond else '***AirTime has null entries other than cancelled/diverted flights***')
    cond = df.AirTime.equals(df.WhOn - df.WhOff - df.TimeZoneDiff)
    print('AirTime column passed check (2/2)' if cond else '***AirTime is not equal to WhOn - WhOff - TimeZoneDiff***')