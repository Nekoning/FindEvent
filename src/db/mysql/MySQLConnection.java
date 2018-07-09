package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import db.DBConnection;
import entity.Item;
import entity.Item.ItemBuilder;
import external.TicketMasterAPI;

import java.util.Set;

import db.DBConnection;
import entity.Item;

public class MySQLConnection implements DBConnection {
	
	private Connection conn;
	
	// private PreparedStatement saveItemStmt = null;
	
	// DB接口经常用singleton
//	private PreparedStatement getSaveItemStmt() {
//		try {
//			if (saveItemStmt == null) {
//				if (conn == null) {
//					System.err.println("DB connection failed!");
//					return null;
//				}
//				saveItemStmt = conn.prepareStatement("INSERT IGNORE INTO items VALUES (?, ?, ?, ?, ?, ?, ?)");
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return saveItemStmt;
//	}
	
	public MySQLConnection() {
		try {
			// 强行要求创建instance
			Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
			conn = DriverManager.getConnection(MySQLDBUtil.URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void setFavoriteItems(String userId, List<String> itemIds) {
		if (conn == null) {
			System.err.println("DB connection failed!");
			return;
		}
		
		try {
			String sql = "INSERT IGNORE INTO history (user_id, item_id) VALUES (?, ?)";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, userId);
			for (String itemId : itemIds) {
				stmt.setString(2,  itemId);
				stmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unsetFavoriteItems(String userId, List<String> itemIds) {
		if (conn == null) {
			System.err.println("DB connection failed!");
			return;
		}
		
		try {
			String sql = "DELETE FROM history WHERE user_id = ? AND item_id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, userId);
			for (String itemId : itemIds) {
				stmt.setString(2,  itemId);
				stmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Set<String> getFavoriteItemIds(String userId) { // history table
		if (conn == null) {
			System.err.println("DB connection failed!");
			return new HashSet<>();
		}
		Set<String> itemIds = new HashSet<>();
		try {
			String sql = "SELECT item_id FROM history WHERE user_id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, userId);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				itemIds.add(rs.getString("item_id"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return itemIds;
	}

	@Override
	public Set<Item> getFavoriteItems(String userId) { // items table
		if (conn == null) {
			System.err.println("DB connection failed!");
			return new HashSet<>();
		}
		Set<Item> favoriteItems = new HashSet<>();
		Set<String> itemIds = getFavoriteItemIds(userId);
		
		try {
			String sql = "SELECT * FROM items WHERE item_id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			for (String itemId : itemIds) {
				stmt.setString(1, itemId);
				ResultSet rs = stmt.executeQuery();
				
				ItemBuilder builder = new ItemBuilder();
				
				// iterator next(): return boolean, check if the current position is valid or not
				while (rs.next()) { 
					builder.setItemId(rs.getString("item_id"));
					builder.setName(rs.getString("name"));
					builder.setAddress(rs.getString("address"));
					builder.setImageUrl(rs.getString("image_url"));
					builder.setUrl(rs.getString("url"));
					builder.setCategories(getCategories(itemId));
					builder.setRating(rs.getDouble("rating"));
					builder.setDistance(rs.getDouble("distance"));
					
					favoriteItems.add(builder.build());
				} 
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return favoriteItems; 
	}

	@Override
	public Set<String> getCategories(String itemId) {
		if (conn == null) {
			System.err.println("DB connection failed!");
			return new HashSet<>();
		}
		Set<String> categories = new HashSet<>();
		try {
			String sql = "SELECT category FROM categories WHERE item_id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, itemId);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				categories.add(rs.getString("category"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return categories;
	}

	@Override
	public List<Item> searchItems(double lat, double lon, String term) {
		TicketMasterAPI tmAPI = new TicketMasterAPI();
		List<Item> items = tmAPI.search(lat, lon, term);
		
		for (Item item : items) {
			saveItem(item);
		}
		
		return items;
	}

	@Override
	public void saveItem(Item item) {
		if (conn == null) {
			System.err.println("DB connection failed!");
			return;
		}

		try {
// 			1. IGNORE: check if item id is in db or not 
//			2. SQL Injection: use input as command 
//			Example 1:
//			SELECT * FROM users WHERE username = '<username>' AND password = '<password>'
//			username: 'aoweifapweofj' OR 1=1 -- (comment)
//			password: 'ahajdhfksfieh'
//			-> 
//			SELECT * FROM users WHERE username = 'aoweifapweofj' OR 1=1 --' AND password = 'ahajdhfksfieh'
//			1=1 -- comment: always true, return all users 
//			Example 2:
//			username: aoweifapweofj OR 1=1 -- (comment)
//			password: 'ahajdhfksfieh' OR '1'='1' 
//			String sql = String.format("INSERT IGNORE INTO items ('%s', '%s', '%s', '%s', '%s', '%s', '%s')", 
//					item.getItemId(), item.getName(), item.getRating(), item.getAddress(), 
//					item.getImageUrl(), item.getUrl(), item.getDistance());
			
			String sql = "INSERT IGNORE INTO items VALUES(?,?,?,?,?,?,?)";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, item.getItemId());
			stmt.setString(2, item.getName());
			stmt.setDouble(3, item.getRating());
			stmt.setString(4, item.getAddress());
			stmt.setString(5, item.getImageUrl());
			stmt.setString(6, item.getUrl());
			stmt.setDouble(7, item.getDistance());		
			stmt.execute();
			
			sql = "INSERT IGNORE INTO categories VALUES (?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, item.getItemId());
			for (String category : item.getCategories()) {
				stmt.setString(2, category);
				stmt.execute();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}

	@Override
	public String getFullname(String userId) {
		if (conn == null) {
			return null;
		}
		String name = "";
		try {
			String sql = "SELECT first_name, last_name from users WHERE user_id = ?";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setString(1, userId);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				name = String.join(" ", rs.getString("first_name"), rs.getString("last_name"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return name;

	}

	@Override
	public boolean verifyLogin(String userId, String password) {
		if (conn == null) {
			return false;
		}
		try {
			String sql = "SELECT user_id from users WHERE user_id = ? and password = ?";
			PreparedStatement statement = conn.prepareStatement(sql);
			statement.setString(1, userId);
			statement.setString(2, password);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;

	}

}
