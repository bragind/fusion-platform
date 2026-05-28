#include <Eigen/Dense>
#include <pybind11/pybind11.h>
#include <pybind11/eigen.h>

namespace py = pybind11;

class KalmanFilter {
public:
    KalmanFilter() {
        // Состояние: [x, y, vx, vy]
        x = Eigen::Vector4d::Zero();
        // Ковариация ошибки оценки
        P = Eigen::Matrix4d::Identity() * 1000.0;

        // Матрица перехода (будет обновляться в predict)
        F = Eigen::Matrix4d::Identity();
        F(0,2) = dt;
        F(1,3) = dt;

        // Матрица управления (ускорения)
        B = Eigen::Matrix<double, 4, 2>::Zero();
        B(0,0) = 0.5 * dt * dt;
        B(1,1) = 0.5 * dt * dt;
        B(2,0) = dt;
        B(3,1) = dt;

        // Матрица наблюдения (только позиция)
        H = Eigen::Matrix<double, 2, 4>::Zero();
        H(0,0) = 1.0;
        H(1,1) = 1.0;

        // Шум процесса
        Q = Eigen::Matrix4d::Identity() * 0.01;
        // Шум измерений GPS
        R = Eigen::Matrix2d::Identity() * 9.0; // 3^2
    }

    void predict(double ax, double ay, double dt) {
        this->dt = dt;
        // Обновляем матрицы, зависящие от dt
        F(0,2) = dt;
        F(1,3) = dt;
        B(0,0) = 0.5 * dt * dt;
        B(1,1) = 0.5 * dt * dt;
        B(2,0) = dt;
        B(3,1) = dt;

        Eigen::Vector2d u(ax, ay);
        x = F * x + B * u;
        P = F * P * F.transpose() + Q;
    }

    void update_gps(double lat, double lon) {
        Eigen::Vector2d z(lat, lon);
        Eigen::Vector2d y = z - H * x;          // инновация
        Eigen::Matrix2d S = H * P * H.transpose() + R;
        Eigen::Matrix<double, 4, 2> K = P * H.transpose() * S.inverse();
        x = x + K * y;
        Eigen::Matrix4d I = Eigen::Matrix4d::Identity();
        P = (I - K * H) * P;
    }

    Eigen::Vector4d get_state() const { return x; }
    Eigen::Vector4d get_covariance_diag() const { return P.diagonal(); }

    void set_state(const Eigen::Vector4d& new_x) { x = new_x; }
    void set_covariance(const Eigen::Matrix4d& new_P) { P = new_P; }

private:
    double dt = 0.01;
    Eigen::Vector4d x;
    Eigen::Matrix4d P;
    Eigen::Matrix4d F;
    Eigen::Matrix<double, 4, 2> B;
    Eigen::Matrix<double, 2, 4> H;
    Eigen::Matrix4d Q;
    Eigen::Matrix2d R;
};

PYBIND11_MODULE(kalman_core, m) {
    m.doc() = "EKF for sensor fusion";
    py::class_<KalmanFilter>(m, "KalmanFilter")
        .def(py::init<>())
        .def("predict", &KalmanFilter::predict)
        .def("update_gps", &KalmanFilter::update_gps)
        .def("get_state", &KalmanFilter::get_state)
        .def("get_covariance_diag", &KalmanFilter::get_covariance_diag)
        .def("set_state", &KalmanFilter::set_state)
        .def("set_covariance", &KalmanFilter::set_covariance);
}