# 내부
PLC_IP = "192.168.94.100"
PLC_PORT = 4545

# DSC
PLC_IP = "192.168.100.151"
PLC_PORT = 6502

* 241216 - 전송 오류 해결 버전
1. 단차 값(data9) 없을 때
2. tracking02에서 검색한 로트(data0)가 tracking01(data0)에 없을 때
3. 허용 범위 외의 값일 때 (-4~2)