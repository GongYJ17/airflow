from airflow.sdk import DAG, task
import pendulum
import datetime
from airflow.providers.http.operators.http import HttpOperator



with DAG( # DAG을 정의한는 부분
    dag_id="dags_http_operator3", # DAG python 파일명과 일치 권장
    schedule=None, # 0시0분 (언제 도는지)
    start_date=pendulum.datetime(2025, 8, 31, tz="Asia/Seoul"), #UTC는 한국시간 +9임.
    catchup=False, # 소급적용을 할건지
) as dag:
    
    get_diplomacy_info = HttpOperator(
        task_id='get_diplomacy_info',
        http_conn_id='openapi.data.go.kr',
        endpoint='/1262000/DiplomacyJournalService/getDiplomacyJournalList',  # 실제 API 엔드포인트
        method='GET',
        data={
        "serviceKey": 'EY9KD4N1rfHjBemZB5rmj/GdCrIwKdmHC2EibAahXYrY/2EW6glZA5kjIoLQ9lpMPSVNkcvqKzSg9tt5y8vhpA==',
        "numOfRows": 10,
        "pageNo": 1
        },
        headers={"Content-Type": "application/json"},
        log_response=True, # 응답 확인 로그를 남겨줌
        do_xcom_push=True
    )


    @task(task_id='python_2')

    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='get_diplomacy_info')
        
        import json
        from pprint import pprint
        
        print("=== XCom 데이터 확인 ===")
        print(f"데이터 존재 여부: {rslt is not None}")
        
        if rslt:
            print(f"데이터 타입: {type(rslt)}")
            
            # 문자열인 경우 JSON 파싱
            if isinstance(rslt, str):
                try:
                    data = json.loads(rslt)
                    print("=== 외교일지 데이터 ===")
                    pprint(data)
                    
                    # 데이터 구조 분석
                    if 'response' in data:
                        header = data['response'].get('header', {})
                        body = data['response'].get('body', {})
                        
                        print(f"\n결과 코드: {header.get('resultCode')}")
                        print(f"결과 메시지: {header.get('resultMsg')}")
                        
                        if body:
                            items = body.get('items', {})
                            if items:
                                item_list = items.get('item', [])
                                print(f"\n총 {body.get('totalCount', 0)}개 중 {len(item_list)}개 조회")
                                
                                # 첫 번째 항목 상세 출력
                                if item_list and len(item_list) > 0:
                                    print("\n=== 첫 번째 외교일지 ===")
                                    first_item = item_list[0] if isinstance(item_list, list) else item_list
                                    for key, value in first_item.items():
                                        print(f"{key}: {value}")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 실패: {e}")
                    print(f"원본 데이터: {rslt[:500]}")
            
            # 이미 dict인 경우
            elif isinstance(rslt, dict):
                print("=== 이미 파싱된 데이터 ===")
                pprint(rslt)
        else:
            print("❌ XCom에 데이터가 없습니다!")
            print("HttpOperator 로그를 확인하세요.")

    # @task(task_id='python_2')
    # def python_2(**kwargs):
    #     ti = kwargs['ti']
    #     rslt = ti.xcom_pull(task_ids='get_deplomacy_info')
    #     import json
    #     from pprint import pprint

    #     pprint(rslt.json())
    


    get_diplomacy_info >> python_2()