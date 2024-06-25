import numpy as np

def calculate_ffmc(temperature, relative_humidity, rain):
    ffmc0 = 85.0
    a = 0.03
    b = 0.0003
    k1 = 1.0
    k2 = 0.0006
    k3 = 0.0012
    k4 = 0.000246
    k5 = 0.000034

    if rain > 0.5:
        er = rain - 0.5
    else:
        er = 0

    if ffmc0 > 90:
        mr = ((ffmc0 - 91.0) * k5 * np.exp(k4 * temperature)) + (10.0 * k2 * (1.0 - np.exp(-k3 * rain))) + (20.0 * (1.0 - np.exp(-k1 * er)))
    else:
        mr = ((ffmc0 - 91.0) * k5 * np.exp(k4 * temperature)) + (10.0 * k2 * (1.0 - np.exp(-k3 * rain * k1 * er)))

    ffmc = 91.9 * (np.exp((mr - 15.0) / (10.0 * (mr + 10.0))) - np.exp(-b * (temperature + 20.0)))
    return ffmc

def calculate_dmc(temperature, rain, ffmc):
    k1 = 0.036
    k2 = 1.0
    if ffmc <= 33.0:
        dmc0 = (0.155 * ffmc) - 43.27
    else:
        dmc0 = (0.305 * ffmc) - 15.87

    if rain > 1.5:
        pr = rain - 1.5
    else:
        pr = 0.0

    dmc = dmc0 + (100.0 * pr)
    return dmc

def calculate_dc(temperature, rain, dmc):
    k1 = 0.27
    k2 = 1.5
    if dmc <= 0.4 * dmc:
        dc0 = (0.27 * dmc) - 2.0
    else:
        dc0 = (0.9 * dmc) - 1.5

    if rain > 2.8:
        pr = rain - 2.8
    else:
        pr = 0.0

    dc = dc0 + (pr * k1)
    return dc

def calculate_isi(wind_speed, ffmc):
    k1 = 0.208
    k2 = 0.0000613
    if wind_speed > 0.0:
        isi = k2 * np.exp(k1 * ffmc * (1.0 - np.exp(-0.5 * wind_speed)))
    else:
        isi = 0.0
    return isi

def calculate_bui(dmc, dc):
    k1 = 0.0322
    k2 = 0.014
    if dc <= 0.4 * dmc:
        bui = (1.0 - k1) * dc
    else:
        bui = ((1.0 - np.exp(-k1 * dmc)) * (0.4 * dmc)) + (((1.0 - (1.0 - k1) * np.exp(-k1 * dmc)) * (dc - 0.4 * dmc)) / (1.0 - np.exp(-k1 * dmc)))
    return bui

def calculate_fwi_from_subindices(isi, bui):
    k1 = 0.4
    k2 = 0.4
    if bui <= 80.0:
        fwi = (0.8 * isi * bui) / (isi + (0.1 * bui))
    else:
        fwi = (2.0 * isi) + (0.1 * bui)
    return fwi

def evaluate_fwi(fwi):
    if fwi < 11.2:
        return "Low"
    elif 11.2 <= fwi < 21.3:
        return "Moderate"
    elif 21.3 <= fwi < 38:
        return "High"
    elif 38 <= fwi < 50:
        return "Very-High"
    else:
        return "Extreme"

def total_fwi(temperature, relative_humidity, rain, wind_speed):
    temperature = temperature  # Celsius to Celsius (no conversion needed)
    wind_speed = wind_speed * 3.6  # m/s to km/h
    ffmc = calculate_ffmc(temperature, relative_humidity, rain)
    dmc = calculate_dmc(temperature, rain, ffmc)
    dc = calculate_dc(temperature, rain, dmc)
    isi = calculate_isi(wind_speed, ffmc)
    bui = calculate_bui(dmc, dc)
    fwi = calculate_fwi_from_subindices(isi, bui)
    risk_level = evaluate_fwi(fwi)
    return fwi, risk_level