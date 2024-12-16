import mysql.connector
import socket
import traceback
import sys
import time

# PLC 접속 정보
PLC_IP = "192.168.100.151"
PLC_PORT = 6502

# MySQL 접속 정보
DB_HOST = "localhost"
DB_USER = "server"
DB_PASSWORD = "dltmxm1234"
DB_NAME = "dataset"

save_id1 = 0
save_id2 = 0

# 데이터베이스 연결 함수
def connect_db():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# 생존
def live1():
    wr1 = b'0020'  # 코드 길이
    wr2 = b'1401'  # 쓰기 명령어
    wr3 = b'D*'    # 변수
    wr4 = b'006900'  # 변수 주소
    wr5 = b'0002'  # 연속 갯수
    wr6 = b'00010001'  # 날짜와 카운트를 합쳐서 bytes로 변환

    live1_send = b'500000FF03FF00' + wr1 + b'0010' + wr2 + b'0000' + wr3 + wr4 + wr5 + wr6 # 쓰기
    time.sleep(0.1)
    s1.sendall(live1_send)

    live1_recv = s1.recv(1024).decode()
    print(f"# 소켓 생존 체크 중")
    return live1_recv

# 현재 ID 불러오기
# /////////////////////////////////// ID 저장
def id1():
    try:
        curs = conn.cursor()
        curs.execute("SELECT id FROM tracking01 ORDER BY id DESC LIMIT 1")
        result = curs.fetchone()
        if result is not None:
            global save_id1
            save_id1 = result[0]
            print(f" 라인1 현재 ID 불러오기 성공: {save_id1}")
        else:
            print("라인1: ID 없음")
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_lines = traceback.extract_tb(exc_traceback)
        tb_last = tb_lines[-1]
        print(f" 라인1 현재 ID 불러오기 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

# /////////////////////////////////// ID 저장
def id2():               
    try:
        curs = conn.cursor()
        curs.execute("SELECT id FROM tracking02 ORDER BY id DESC LIMIT 1")
        result = curs.fetchone()
        if result is not None:
            global save_id2
            save_id2 = result[0]
            print(f" 라인2 현재 ID 불러오기 성공: {save_id2}")
        else:
            print("라인2: ID 없음")
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_lines = traceback.extract_tb(exc_traceback)
        tb_last = tb_lines[-1]
        print(f" 라인2 현재 ID 불러오기 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")


# PLC 단차값 읽기
def receive(s1):
    wr1 = b'0018'  # 코드 길이
    wr2 = b'0401'  # 읽기 명령어
    wr3 = b'D*'    # 변수
    wr4 = b'006504'  # 변수 주소
    wr5 = b'0002'  # 연속 갯수

    receive_send = b'500000FF03FF00' + wr1 + b'0010' + wr2 + b'0000' + wr3 + wr4 + wr5
    s1.sendall(receive_send)

    receive_recv = s1.recv(1024).decode()

    # print(f"{receive_recv}") # 단차 읽어온 값
    return receive_recv

# DB에 단차값 저장
def receive_and_save():
        plc_read = receive(s1)  # PLC에서 데이터 읽기
        
        # 6504 16진수 4자리
        plc_value_16 = plc_read[-8:-4]

        # 값이 있으면 단차값 저장
        if (plc_value_16 != '0000'):
            conn = connect_db()
            curs = conn.cursor()

            curs.execute("SELECT id, data9 FROM tracking01 ORDER BY id DESC LIMIT 1")
            current_record = curs.fetchone()
            current_id3 = current_record[0]
            current_data9 = current_record[1]
            
            # 16진수에서 10진수로 변환
            plc_value_10 = int(plc_value_16, 16)          

            # 16비트 부호 있는 정수로 해석
            if plc_value_10 >= 32768:  # 2^15
                plc_value_10 -= 65536  # 2^16

            curs.execute("UPDATE tracking01 SET data9 = %s WHERE id = %s", (plc_value_10, current_id3))
            conn.commit()
            # print("단차 저장")

             # data9 값에 따라 data8 업데이트
            if current_data9 is None or not (-4 < current_data9 < 2):
                curs.execute("UPDATE tracking01 SET data8 = 2 WHERE id = %s", (current_id3,))
            else:
                curs.execute("UPDATE tracking01 SET data8 = 1 WHERE id = %s", (current_id3,))

            # 단차값 저장했다면 PLC 비우기
            if True:
                wr1 = b'0020'  # 코드 길이
                wr2 = b'1401'  # 쓰기 명령어
                wr3 = b'D*'    # 변수
                wr4 = b'006504'  # 변수 주소
                wr5 = b'0002'  # 연속 갯수
                wr6 = b'00000000'  # 날짜와 카운트를 합쳐서 bytes로 변환

                clear_send = b'500000FF03FF00' + wr1 + b'0010' + wr2 + b'0000' + wr3 + wr4 + wr5 + wr6 # 쓰기
                time.sleep(0.1)
                s1.sendall(clear_send)

                clear_recv = s1.recv(1024).decode()

                print(f"################################### PLC 단차 6504 6505 비움")
                print(f"################################### 받은값: {plc_value_16} 저장한값:{plc_value_10}")
            # print(f"{plc_value_16}")

            curs.close()

        else:
            print("PLC 단차 데이터 기다리는중... 6504 6505 읽는중")

#값 전송하기
def send(s1, address, send_data):
    wr1 = b'0020'  # 코드 길이
    wr2 = b'1401'  # 쓰기 명령어
    wr3 = b'D*'    # 변수
    wr4 = address  # 변수 주소
    wr5 = b'0002'  # 연속 갯수
    wr6 = send_data  # 날짜와 카운트를 합쳐서 bytes로 변환

    SocSend_ = b'500000FF03FF00' + wr1 + b'0010' + wr2 + b'0000' + wr3 + wr4 + wr5 + wr6
    s1.sendall(SocSend_)

    SocRecv = s1.recv(1024).decode()
    if len(SocRecv) <= 0:
        print(f" _for break")
        return

# 6500 6501
def send_plc1():
    data1 = b'00010001'  # 4자리가 1개 데이터
    send(s1, b'006500', data1)
    print(f"==================================== PLC 1-1(확인값) 6500 6501 전송")

# //////////////////////////////////// 라인1_2 보내기 (날짜/카운트)
def send_plc2():
    conn = connect_db()
    curs = conn.cursor()
    curs.execute("SELECT data0 FROM tracking01 ORDER BY id DESC LIMIT 1")
    result = curs.fetchone()
    if (result[0] != None):
        write_value = result[0]  # DB에서 읽어온 값

        # write_value에서 날짜와 카운트 추출
        month_code = write_value[2:3]  # 'A'
        day = write_value[3:5]  # '15'
        count = write_value[8:12]  # '0001'

        # 월 코드 변환
        month = ''
        if month_code == 'A':
            month = '10'  # 10월
        elif month_code == 'B':
            month = '11'  # 11월
        elif month_code == 'C':
            month = '12'  # 12월
        else:
            month = month_code  # 1~9월은 그대로 사용

        # 날짜값으로 변환
        date = f"{month.zfill(2)}{day.zfill(2)}"  # '1015'
        
        # 날짜를 8자리 16진수로 변환
        date_hex = f"{int(date):04X}"
        count_hex = f"{int(count):04X}"

        # 최종 데이터 생성
        string_data = (date_hex + count_hex) # '03F70001'

        # 바이트로 변환해서 저장
        data2 = string_data.encode()

        send(s1, b'006502', data2)
        print(f"==================================== PLC 1-2(날짜,카운트) 6502 6503 전송")
    else:
        print("tracking01 ID 없음")

    curs.close()

    # //////////////////////////////////// 라인2-1에 보내기 (확인값)
def send_plc3():
    conn = connect_db()
    curs = conn.cursor()

    # tracking02에서 가장 최근 데이터 가져오기
    curs.execute("SELECT data0 FROM tracking02 ORDER BY id DESC LIMIT 1")
    current_record = curs.fetchone()

    if current_record is not None:
        tracking02_data0 = current_record[0]  # data0 컬럼이 첫 번째 열이라고 가정

        # tracking01에서 tracking02_data0과 같은 data0 값 검색
        curs.execute("SELECT data9 FROM tracking01 WHERE data0 = %s ORDER BY id DESC LIMIT 1", (tracking02_data0,) )
        check_record = curs.fetchone()
        
        # check_record가 None이면 tracking01에 data0이 없음
        if check_record is not None and check_record[0] is not None:
            data9 = int(check_record[0])

            # data9가 -4와 2 사이인지 확인
            if -4 <= data9 <= 2:
                data3 = b'00010001'  # 조건에 맞으면
                print(f"111111111111111111111111111111111111 값이 조건에 맞음: {data9}")
            else:
                data3 = b'00020002'  # 조건에 맞지 않으면
                print(f"222222222222222222222222222222222222 값이 비었거나 조건에 맞지 않음: {data9}")
        else:
            data3 = b'00020002'  # tracking01에 data0이 없음
            print(f"222222222222222222222222222222222222 단차 측정을 하지 않은 제품임 ")

        send(s1, b'006600', data3)
        print(f"==================================== PLC 2-1(확인값) 6600 6601 전송: {data3.decode()}")
    else:
        print("용접의 스캔값을 읽을 수 없음")

    curs.close()




# //////////////////////////////////// 라인2-2에 보내기 (날짜/카운트)
def send_plc4():
    conn = connect_db()
    curs = conn.cursor()
    curs.execute("SELECT data0 FROM tracking02 ORDER BY id DESC LIMIT 1")
    result = curs.fetchone()
    if (result[0] != None):
        write_value = result[0]  # DB에서 읽어온 값

        # write_value에서 날짜와 카운트 추출
        month_code = write_value[2:3]  # 'A'
        day = write_value[3:5]  # '15'
        count = write_value[8:12]  # '0001'

        # 월 코드 변환
        month = ''
        if month_code == 'A':
            month = '10'  # 10월
        elif month_code == 'B':
            month = '11'  # 11월
        elif month_code == 'C':
            month = '12'  # 12월
        else:
            month = month_code  # 1~9월은 그대로 사용

        # 날짜값으로 변환
        date = f"{month.zfill(2)}{day.zfill(2)}"  # '1015'
        
        # 날짜를 8자리 16진수로 변환
        date_hex = f"{int(date):04X}"
        count_hex = f"{int(count):04X}"

        # 최종 데이터 생성
        string_data = (date_hex + count_hex) # '000003F700000001'

        # 바이트로 변환해서 저장
        data4 = string_data.encode()

        send(s1, b'006602', data4)
        print(f"==================================== PLC 2-2(날짜, 카운트) 6602 6603 전송")
    else:
        print("tracking02 ID 없음")

    curs.close()

# PLC 연동
try:

    conn = connect_db()
    print("DB 연결")

    #현재 ID 불러오기
    id1()
    id2()

    while True:
        time.sleep(0.1)

        # 소켓 연결
        try:
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s1.settimeout(5.0)
            s1.connect((PLC_IP, PLC_PORT))
            # print("소켓 연결")

            # 데이터베이스 연결
            try:
                conn = connect_db()
                # print("DB 연결")

                #연결 성공했다면
                if(True):
                    try:
                        
                        live1()

                        try:

                            if(True):
                                
                                #읽기
                                try:
                                    receive_and_save()
                                except Exception as e:
                                    exc_type, exc_value, exc_traceback = sys.exc_info()
                                    tb_lines = traceback.extract_tb(exc_traceback)
                                    tb_last = tb_lines[-1]
                                    print(f" 읽기 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

                                #전송
                                try:
                                    conn = connect_db()
                                    curs = conn.cursor()

                                    curs.execute("SELECT id FROM tracking01 ORDER BY id DESC LIMIT 1")
                                    current_record1 = curs.fetchone()

                                    if current_record1 is not None:
                                        current_id1 = current_record1[0]
                                        if current_id1 > save_id1:
                                            try:
                                                send_plc1()
                                                send_plc2()
                                                save_id1 = current_id1
                                            except Exception as e:
                                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                                tb_lines = traceback.extract_tb(exc_traceback)
                                                tb_last = tb_lines[-1]
                                                print(f" tracking01 전송 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")
                                        else:
                                            print('tracking01 ID 체크 중')
                                    else:
                                        print('tracking01 데이터 없음')


                                    # tracking02 ID 체크
                                    curs.execute("SELECT id FROM tracking02 ORDER BY id DESC LIMIT 1")
                                    current_record2 = curs.fetchone()
                                    if current_record2 is not None:
                                        current_id2 = current_record2[0]
                                        if current_id2 > save_id2:
                                            try:
                                                time.sleep(0.1)
                                                send_plc3()
                                                send_plc4()
                                                save_id2 = current_id2
                                            except Exception as e:
                                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                                tb_lines = traceback.extract_tb(exc_traceback)
                                                tb_last = tb_lines[-1]
                                                print(f" tracking02 전송 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")
                                        else:
                                            print('tracking01 ID 체크 중')
                                    else:
                                        print('tracking02 데이터 없음')

                                except Exception as e:
                                    exc_type, exc_value, exc_traceback = sys.exc_info()
                                    tb_lines = traceback.extract_tb(exc_traceback)
                                    tb_last = tb_lines[-1]
                                    print(f" 전송 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

                        except Exception as e:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            tb_lines = traceback.extract_tb(exc_traceback)
                            tb_last = tb_lines[-1]
                            print(f" 현재 ID 불러오기 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")


                    except Exception as e:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        tb_lines = traceback.extract_tb(exc_traceback)
                        tb_last = tb_lines[-1]
                        print(f" 생존 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_lines = traceback.extract_tb(exc_traceback)
                tb_last = tb_lines[-1]
                print(f" DB 연결 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_lines = traceback.extract_tb(exc_traceback)
            tb_last = tb_lines[-1]
            print(f" 소켓 연결 실패 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb_lines = traceback.extract_tb(exc_traceback)
    tb_last = tb_lines[-1]
    print(f" 에러 {time.strftime('%H:%M:%S')} {str(e)} / {tb_last.lineno} / {tb_last.line}")

finally:
    s1.close()
    input(" _System OFF")