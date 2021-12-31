package spring.batch.part4;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "user_name")
    private String userName;

    @Column(name = "user_total_purchase")
    private int totalPurchase;

    @Column(name = "user_grade")
    @Enumerated(EnumType.STRING)
    private UserGrade userGrade;

    public User(String userName, int totalPurchase) {
        this.userName = userName;
        this.totalPurchase = totalPurchase;
        countUserGrade();
    }

    private UserGrade makePurchase(int purchase) {
        totalPurchase =+ purchase;
        countUserGrade();
        return userGrade;
    }

    private void countUserGrade() {
        if(totalPurchase < 200000) {
            userGrade = UserGrade.NORMAL;
        }
        else if (totalPurchase >= 200000 && totalPurchase < 300000) {
            userGrade = UserGrade.SILVER;
        }
        else if (totalPurchase > 300000 && totalPurchase < 500000) {
            userGrade = UserGrade.GOLD;
        }
        else if (totalPurchase > 500000) {
            userGrade = UserGrade.VIP;
        }
    }
}
