WITH propfit AS(
    SELECT c.*,
       d.os,
       d.deviceMake,
       d.deviceType,
       d.deviceTypeD,
       d.deviceModel,
       d.tagid,
       d.appCategory,
       d.bidfloor,
       d.country,
       d.region,
       d.lat,
       d.lon
    FROM cream.propfit_daily_raw c
    LEFT JOIN (
            SELECT
                    tid,
    --                 BidStream.DeviceId,
                    LOWER(BidStream.request.device.os) AS os,
                    LOWER(BidStream.request.device.make) AS deviceMake,
                    BidStream.request.device.devicetype AS deviceType,
                    CASE
                            WHEN BidStream.request.device.devicetype = '1' THEN 'Mobile/Tablet'
                            WHEN BidStream.request.device.devicetype = '2' THEN 'PC'
                            WHEN BidStream.request.device.devicetype = '3' THEN 'Connected TV'
                            WHEN BidStream.request.device.devicetype = '4' THEN 'Phone'
                            WHEN BidStream.request.device.devicetype = '5' THEN 'Tablet'
                            WHEN BidStream.request.device.devicetype = '6' THEN 'Connected Device'
                            WHEN BidStream.request.device.devicetype = '7' THEN 'Set Top Box'
                            ELSE BidStream.request.device.devicetype END AS deviceTypeD,
                    LOWER(BidStream.request.device.model) AS deviceModel,
                    BidStream.request.imp.tagid[0] AS tagid,
                    BidStream.bundle_domain AS url,
                    CASE
                        WHEN REPLACE(b.category01, ' ', '') IN ('Books&Reference', '도서/참고자료', '도서') THEN '1'                                                   --'도서'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Business', '비즈니스') THEN '2'                                                                      --'비즈니스'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Tools', '도구', 'Libraries&Demo', '라이브러리/데모', '개발자도구') THEN '3'                            --'개발자 도구'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Education', '학습', '교육') THEN '4'                                                                 --'교육'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Entertainment', '엔터테인먼트') THEN '5'                                                              --'엔터테인먼트'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Finance', '금융') THEN '6'                                                                           --'금융'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Food&Drink', '식음료', '음식및음료') THEN '7'                                                         --'음식 및 음료'
                        WHEN REPLACE(b.category01, ' ', '') IN ('Action', '액션', 'Adventure', '어드벤처', 'Arcade', '아케이드', 'Board', '보드', 'Bubbleshooter',
                                                                'Card', '카드', 'Casino', '카지노', 'Casual', '캐주얼게임', '캐주얼', 'Educational', 'Multiplayer',
                                                                'Music', 'Puzzle', '퍼즐', 'Racing', '자동차경주', '레이싱', 'RolePlaying', '롤플레잉', 'Simulation',
                                                                '시뮬레이션', 'Singleplayer', '싱글플레이어', 'Sports', 'Strategy', '전략', 'Stylized', 'Trivia',
                                                                'Tycoon', 'Word', '단어', '퀴즈', '게임') THEN '8'                                                     --'게임'
                        WHEN b.category01 IN ('Art & Design', '예술/디자인', '그래픽 및 디자인') THEN '8'                                                         --'그래픽 및 디자인'
                        WHEN b.category01 IN ('Health & Fitness', '건강/운동', '건강 및 피트니스') THEN '9'                                                       --'건강 및 피트니스'
                        WHEN b.category01 IN ('Lifestyle', '라이프 스타일', '라이프스타일') THEN '10'                                                             --'라이프 스타일'
                        WHEN b.category01 IN ('News & Magazines', '뉴스', '뉴스/잡지', '뉴스 및 잡지') THEN '11'                                                  --'뉴스 및 잡지'
                        WHEN b.category01 IN ('Medical', '의료') THEN '12'                                                                                      --'의료'
                        WHEN b.category01 IN ('Music & Audio', '음악/오디오', '음악') THEN '13'                                                                  --'음악'
                        WHEN b.category01 IN ('Navigation', 'Maps & Navigation', '지도/내비게이션', '내비게이션') THEN '14'                                       --'내비게이션'
                        WHEN b.category01 IN ('Photography', '사진', 'Video Players & Editors', '동영상 플레이어/편집기', '사진 및 비디오') THEN '15'              --'사진 및 비디오'
                        WHEN b.category01 IN ('Productivity', '참고', '생산성') THEN '16'                                                                       --'생산성'
                        WHEN b.category01 IN ('Shopping', '쇼핑') THEN '17'                                                                                     --'쇼핑'
                        WHEN b.category01 IN ('Social', '소셜', '스티커', '소셜 네트워킹') THEN '18'                                                              --'소셜 네트워킹'
                        WHEN b.category01 IN ('Sports', '스포츠') THEN '19'                                                                                     --'스포츠'
                        WHEN b.category01 IN ('Travel & Local', '여행 및 지역정보', '여행') THEN '20'                                                            --'여행'
                        WHEN b.category01 IN ('Personalization', '맞춤 설정', '유틸리티') THEN '21'                                                              --'유틸리티'
                        WHEN b.category01 IN ('Weather', '날씨') THEN '22'                                                                                      --'날씨'
                        WHEN b.category01 IN ('Auto & Vehicles', '자동차', '자동차 및 차량') THEN '23'                                                           --'자동차 및 차량'
                        WHEN b.category01 IN ('Beauty', '뷰티') THEN '24'                                                                                       --'뷰티'
                        WHEN b.category01 IN ('Comics', '만화') THEN '25'                                                                                       --'만화'
                        WHEN b.category01 IN ('Communications', 'Communication', '커뮤니케이션') THEN '26'                                                       --'커뮤니케이션'
                        WHEN b.category01 IN ('Dating', '데이트') THEN '27'                                                                                     --'데이트'
                        WHEN b.category01 IN ('Events', '이벤트', '예약 및 예매') THEN '28'                                                                      --'예약 및 예매'
                        WHEN b.category01 IN ('House & Home', '부동산/홈 인테리어', '부동산 및 인테리어') THEN '29'                                                --'부동산 및 인테리어'
                        WHEN b.category01 IN ('Parenting', '출산/육아', '육아') THEN '30'                                                                        --'육아'
                        ELSE REPLACE(b.category01, ' ', '') END AS appCategory,
                    FORMAT_NUMBER(BidStream.bidfloor, 12) AS bidfloor,
                    BidStream.request.device.geo.country,
                    CASE
                        WHEN LOWER(BidStream.request.device.geo.region) = 'kr' THEN 'KR-0'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('11', "ch'angsa-dong", 'dobong-gu', 'dongdaemun-gu', 'dongjak-gu',
                                'eunpyeong-gu', 'gangbuk-gu', 'gangdong-gu', 'gangnam-gu', 'gangseo-gu', 'geumcheon-gu', 'guro-gu', 'gwanak-gu',
                                'gwangjin-gu', 'hapjeong-dong', "ich'on-dong", 'jongno-gu', 'junggu', 'jung-gu', 'jungnang-gu',
                                'kr-11', 'mapo-gu', 'noryangjin-dong', 'nowon-gu', 'seocho', 'seocho-gu', 'seodaemun-gu', 'seongbuk-gu',
                                'seongdong-gu', 'seoul', "seoul_t'ukpyolsi", 'seoul-teukbyeolsi', 'sinsa-dong', 'soeul', 'songpa-gu', 'yangcheon-gu',
                                "yangch'on-gu", 'yeongdeungpo-gu', 'yongsan-gu') THEN 'KR-11'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('26', 'buk-gu', 'busan', 'busan-gwangyeoksi', 'busanjin-gu', 'donggu',
                                'dong-gu', 'geumjeong-gu', 'gijang', 'gijang-gun', 'haeundae', 'haeundae-gu', 'kr-26', 'nam-gu', 'saha-gu',
                                'sasang-gu', 'seo-gu', 'yeongdo-gu', 'yeonje-gu') THEN 'KR-26'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('27', 'daegu', 'daegu-gwangyeoksi', 'dalseo-gu', 'dalseong-gun', 'kr-27',
                                'suseong-gu', 'suyeong-gu') THEN 'KR-27'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('28', 'bupyeong', 'bupyeong-gu', 'galsan', 'ganghwa-gun', 'gyeyang-gu',
                                'incheon', 'incheon-gwangyeoksi', 'inhyeondong', 'juan-dong', 'kr-28', 'namdong-gu', 'ongjin-gun', 'unseodong',
                                'yeonsu-gu') THEN 'KR-28'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('29', 'gwangju', 'gwangju-gwangyeoksi', 'gwangsan-gu', 'kr-29') THEN 'KR-29'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('30', 'daedeok-gu', 'daejeon', 'daejeon-gwangyeoksi', 'kr-30', 'yuseong',
                                'yuseong-gu') THEN 'KR-30'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('31', 'kr-31', 'ulju-gun', 'ulsan', 'ulsan-gwangyeoksi') THEN 'KR-31'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('41', 'ansan-si', 'anseong', 'anyang-si', 'areannamkwaengi', 'bucheon-si',
                                'bundang-gu', 'dongan', 'dongducheon-si', 'gapyeong county', 'gimpo-si', 'goyang-si', 'gunpo', 'guri-si', 'gwacheon',
                                'gwacheon-si', 'gwangmyeong', 'gwangmyeong-si', 'gyeonggi', 'gyeonggi-do', 'hanam', 'hwaseong-si', 'icheon-si',
                                'jinjeop-eup', 'kosaek-tong', 'kr-41', 'masan-dong', 'namyangju', 'osan', 'paju', 'paju-si', 'pocheon', 'pocheon-si',
                                'pyeongtaek-si', 'seongnam-si', 'siheung-si', 'suji-gu', 'suwon', 'tokyang-gu', 'uijeongbu-si',
                                'uiwang', 'uiwang-si', 'yangju', "yangp'yong", 'yeoju', 'yeoncheon', 'yeoncheon-gun', 'yongin', 'yongin-si') THEN 'KR-41'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('42', 'cheorwon', "chinch'on", 'chuncheon', 'donghae-si', 'gangneung',
                                'gangwon', 'gangwon-do', 'goseong-gun', 'hoengseong-gun', 'hongcheon-gun', 'hwacheon', 'inje-gun', 'jeongseon-gun',
                                'kr-42', 'pyeongchang', 'samcheok', 'samcheok-si', 'sokcho', 'taebaek-si', 'tonghae', 'wonju', 'yanggu', 'yangyang',
                                'yangyang-gun', 'yeongwol-gun') THEN 'KR-42'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('43', 'boeun-gun', 'cheongju-si', 'cheongwon-gun', 'cheorwon-gun', 'chungbuk',
                                'chungcheongbuk-do', 'chungju', 'danyang', 'danyang-gun', 'eumseong', 'eumseong-gun', 'heungdeok-gu', 'jecheon',
                                'koesan', 'kr-43', 'okcheon', 'okcheon-gun', 'yeongdong', 'yeongdong-gun', 'yongam') THEN 'KR-43'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('44', 'asan', 'boryeong', 'boryeong-si', 'buyeo-gun', 'cheonan', 'cheongyang-gun',
                                'chungcheongnam-do', 'chungnam', 'geumsan', 'geumsan-gun', 'gongju', 'gyeryong', 'gyeryong-si', "hongch'on", 'hongseong',
                                'hongseong-gun', 'kr-44', 'nonsan', 'north chungcheong', 'seocheon-gun', 'seosan city', 'taean-gun', 'taian', 'ungcheon-eup',
                                'yesan') THEN 'KR-44'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('45', 'buan-gun', 'changsu', 'gimje-si', 'gochang', 'gunsan', 'iksan', 'imsil',
                                'jeollabuk-do', 'jeonbuk', 'jeongeup', 'jeonju', 'jinan-gun', 'kimje', "koch'ang", 'kr-45', 'muju-gun', 'samnye',
                                'sunchang-gun', 'wanju') THEN 'KR-45'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('46', 'boseong', 'boseong-gun', 'daeguri', 'damyang', 'damyang-gun', 'gangjin-gun',
                                'goheung-gun', 'gokseong-gun', 'gurye', 'gurye-gun', 'gwangyang', 'gwangyang-si', 'haenam', 'haenam-gun',
                                'hampyeong-gun', 'hwasun-gun', 'jangheung', 'jangseong', 'jeollanam-do', 'jeonnam', 'jindo-gun', 'kr-46', 'kurye', 'mokpo',
                                'mokpo-si', 'muan', 'naju', 'suncheon', 'suncheon-si', 'wando-gun', 'yeongam', 'yeongam-gun', 'yeonggwang', 'yeonggwang-gun',
                                'yeosu') THEN 'KR-46'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('47', 'andong', 'bonghwa-gun', 'cheongdo-gun', 'cheongsong', 'cheongsong gun',
                                'chilgok', 'chilgok-gun', 'gimcheon', 'goryeong-gun', 'gumi', 'gunwi-gun', 'gyeongbuk', 'gyeongju', 'gyeongsangbuk-do',
                                'gyeongsan-si', 'kr-47', 'mungyeong', 'pohang', 'pohang-si', 'sangju', 'seongju-gun', 'uiseong-gun', 'ulchin', 'uljin county',
                                'yecheon-gun', 'yeongcheon-si', 'yeongdeok', 'yeongdeok-gun', 'yeongju', 'yeongyang-gun') THEN 'KR-47'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('48', 'changnyeong', 'changwon', 'geochang-gun', 'geoje', 'geoje-si', 'gimhae',
                                'gyeongnam', 'gyeongsangnam-do', 'hadong', 'haman', 'haman-gun', 'hamyang-gun', 'hapcheon-gun', 'jinhae-gu',
                                'jinju', 'kr-48', 'miryang', 'namhae', 'namhae-gun', 'sacheon-si', 'sancheong-gun', 'tongyeong-si', 'uiryeong-gun', 'yangsan') THEN 'KR-48'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('49', 'jeju', 'jeju city', 'jeju-do', 'jeju-teukbyeoljachido', 'kr-49', 'seogwipo',
                                'seogwipo-si') THEN 'KR-49'
                        WHEN LOWER(BidStream.request.device.geo.region) IN ('50', 'kr-50', 'sejong', 'sejong-si') THEN 'KR-50'
                        ELSE NULL END AS region,
                    FORMAT_STRING(BidStream.request.device.geo.lat) AS lat,
                    FORMAT_STRING(BidStream.request.device.geo.lon) AS lon
            FROM ice.streams_bid_bronze_app_nhn a
            LEFT JOIN (SELECT requestAppBundle, category01 from ice.app_info_nhn) b ON a.BidStream.bundle_domain = b.requestAppBundle
            WHERE a.actiontime_date BETWEEN '2023-03-03' AND '2023-03-31'
    ) d ON c.tid = d.tid AND c.Date BETWEEN '2023-03-03' AND '2023-03-31'
    WHERE ad_adv = '5'
)

SELECT Date,
       propfit.DeviceId AS DeviceId,
       propfit.deviceType AS deviceType,
       propfit.bidfloor AS bidfloor,
       propfit.tid AS tid,
       CASE WHEN bidtime IS NOT NULL THEN '1' ELSE NULL END AS bid,
       CASE WHEN bidtime IS NOT NULL AND imptime IS NOT NULL THEN '1' ELSE '0' END AS imp,
       CASE WHEN bidtime IS NOT NULL AND imptime IS NOT NULL AND clktime IS NOT NULL THEN '1' ELSE '0' END AS clk,
       CASE WHEN Width = '320' AND Height = '480' THEN '1' WHEN Width = '320' AND Height = '50' THEN '2' ELSE NULL END AS size,
       CASE WHEN os = 'android' THEN '1' WHEN os = 'ios' THEN '2' WHEN os = 'unknown' THEN '4' ELSE '3' END AS os,
       CASE WHEN appCategory = '' THEN NULL ELSE appCategory END AS appCategory,
       regexp_replace(region, 'KR-', '') AS region
FROM propfit
