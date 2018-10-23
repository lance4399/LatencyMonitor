package helper;

import java.util.Locale;
import java.util.ResourceBundle;

public class ConfigHelper {

	/**  Es **/
	public static String Elasticseach_Host="xx.xx.xx.xxx";
	public static int Elasticseach_HttpPort = 9200;
	public static String Index_Name = "dwd_latency_monitor";
	public static String Type_Name = "dwd_latency";

	/**  Kafka **/
	public static String Rawlog_Ev_Topic = "wap_ev";

	public static String Dwd_Pv_Topic = "wap_pv_dwd";
	public static String Dwd_Ev_Topic = "wap_se_dwd";
	public static String Dwd_Click_Topic = "wap_action_dwd";


	public static  int Threshold = 30;

	static {

////		Locale locale = new Locale("zh","CN");
////		ResourceBundle bundle = ResourceBundle.getBundle("src/applications",locale);
//		ResourceBundle bundle = ResourceBundle.getBundle("applications");
//
//		Elasticseach_Host = bundle.getString("elasticseach_host");
//		Elasticseach_HttpPort = Integer.parseInt( bundle.getString("elasticseach_http_port"));
//		Index_Name = bundle.getString("index_name");
//		Type_Name = bundle.getString("type_name");
//
//		Rawlog_Ev_Topic = bundle.getString("rawlog_ev_topic");
//
//		Dwd_Pv_Topic = bundle.getString("dwd_pv_topic");
//		Dwd_Ev_Topic = bundle.getString("dwd_ev_topic");
//		Dwd_Click_Topic = bundle.getString("dwd_click_topic");

	}

	public static void main(String[] args) {

		System.out.println(Elasticseach_Host);
	}
}
