package cn.net.ycloud.example;

import java.io.UnsupportedEncodingException;

public class Test {
public static void main(String[] args) throws UnsupportedEncodingException {
	
	System.out.println(java.net.URLEncoder.encode("\t","utf-8"));
	
//	String[] list=new String[]{"20171008","20171009"};
//
//	
//	for(String s:list)
//	{
//		System.out.println("insert overwrite table  ydbpartion  select 'NB_TAB_DEVICEDATA', '"+s+"', '',     YROW( 'USERID',USERID, 'USERTYPE',USERTYPE, 'LOCATION',LOCATION, 'APPLICATION',APPLICATION, 'APPID',APPID, 'NBDOMAIN',NBDOMAIN, 'BEHAVIOR',BEHAVIOR, 'AUTHTYPE',AUTHTYPE, 'AUTHACCOUNT',AUTHACCOUNT, 'PERIOD',PERIOD, 'NBDATASOURCE',NBDATASOURCE, 'TIME',TIME, 'NBKEYWORD',NBKEYWORD, 'NBKEYWORDID',NBKEYWORDID, 'TERMINALID',TERMINALID, 'TERMINALNAME',TERMINALNAME, 'TGDID',TGDID, 'NBSTORE',NBSTORE, 'TRASHRULEID',TRASHRULEID, 'OBJECTID',OBJECTID, 'ID',ID, 'CLUE_ID',CLUE_ID, 'UPAREAID',UPAREAID, 'CLUE_SRC_SYS',CLUE_SRC_SYS, 'CLUE_DST_SYS',CLUE_DST_SYS, 'ISP_ID',ISP_ID, 'DEVICE_ID',DEVICE_ID, 'LINEID',LINEID, 'SRC_IP',SRC_IP, 'DST_IP',DST_IP, 'SRC_IPID',SRC_IPID, 'DST_IPID',DST_IPID, 'STRSRC_IP',STRSRC_IP, 'STRDST_IP',STRDST_IP, 'BSATTRIBUTION',BSATTRIBUTION, 'PHONEATTRIBUTION',PHONEATTRIBUTION, 'TEID',TEID, 'SRC_PORT',SRC_PORT, 'DST_PORT',DST_PORT, 'MAC',MAC, 'CAPTURE_TIME',CAPTURE_TIME, 'DATA_SOURCE',DATA_SOURCE, 'COMPANY_NAME',COMPANY_NAME, 'COUNTRY_TYPE',COUNTRY_TYPE, 'CERTIFICATE_CODE',CERTIFICATE_CODE, 'CERTIFICATE_TYPE',CERTIFICATE_TYPE, 'SESSIONID',SESSIONID, 'AUTH_TYPE',AUTH_TYPE, 'RELATEDIRECTION',RELATEDIRECTION, 'AUTH_ACCOUNT',AUTH_ACCOUNT, 'IMSI',IMSI, 'EQUIPMENT_ID',EQUIPMENT_ID, 'HARDWARE_SIGNATURE',HARDWARE_SIGNATURE, 'SERVICECODE',SERVICECODE, 'BASE_STATION_ID',BASE_STATION_ID, 'CONTEXT',CONTEXT, 'LONGITUDE',LONGITUDE, 'LATITUDE',LATITUDE, 'USER_AGENT',USER_AGENT, 'PRO_TYPE',PRO_TYPE, 'PAR_TYPE',PAR_TYPE, 'APP_TYPE',APP_TYPE, 'APP_ID',APP_ID, 'USERNAME',USERNAME, 'NICK_NAME',NICK_NAME, 'PHOTO_URL',PHOTO_URL, 'AP_MAC',AP_MAC, 'AC_MAC',AC_MAC, 'HARDWARE_TYPE',HARDWARE_TYPE, 'SIGNAL_INTENSITY',SIGNAL_INTENSITY, 'CRC_ERROR',CRC_ERROR, 'TERM_TYPE',TERM_TYPE, 'SSID',SSID, 'CHANNEL',CHANNEL, 'REMARK',REMARK, 'GPS_ERROR',GPS_ERROR, 'SYS_VERSION',SYS_VERSION, 'TEL_AREA_CODE',TEL_AREA_CODE, 'PHONE_SEVEN_NUMBER',PHONE_SEVEN_NUMBER, 'MAINFILE',MAINFILE, 'FILESIZE',FILESIZE, 'ITEMFLAG',ITEMFLAG, 'RECORDFLAG',RECORDFLAG, 'SUPERCLASS',SUPERCLASS, 'SUBCLASS',SUBCLASS, 'REMARKSTR',REMARKSTR, 'C1',C1, 'C2',C2, 'I1',I1, 'I2',I2, 'SRC_AUTH_ACCOUNT',SRC_AUTH_ACCOUNT, 'DATALEVEL',DATALEVEL, 'ACTIVEAREAID',ACTIVEAREAID, 'SECURITY_SOFTWARE_ORGCODE',SECURITY_SOFTWARE_ORGCODE, 'RECORD_ID',RECORD_ID, 'SRC_FIELDINFO',SRC_FIELDINFO, 'INTERRUPT_FILE',INTERRUPT_FILE, 'APPLICATION_LAYER_PROTOCOL',APPLICATION_LAYER_PROTOCOL, 'PNAME',PNAME, 'PNATION',PNATION, 'THREE_STATION_ID',THREE_STATION_ID, 'AP_ID',AP_ID, 'C3',C3, 'C4',C4, 'C5',C5, 'mortonhash',YMortonHash(LONGITUDE,LATITUDE)  )  from  NB_TAB_DEVICEDATA_hive_fenqu where thedate='"+s+"';");
//	}
}
}
