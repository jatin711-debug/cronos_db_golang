import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { LoginView } from "@/views/LoginView";
import * as api from "@/lib/api";

describe("LoginView", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("renders the login form", () => {
    render(
      <MemoryRouter>
        <LoginView />
      </MemoryRouter>,
    );

    expect(screen.getByText("Admin Sign In")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("eyJhbGciOi...")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /sign in/i })).toBeInTheDocument();
  });

  it("shows error for empty token", () => {
    render(
      <MemoryRouter>
        <LoginView />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: /sign in/i }));
    expect(screen.getByText("Please paste a JWT token.")).toBeInTheDocument();
  });

  it("stores token and navigates to dashboard", () => {
    const setTokenSpy = vi.spyOn(api, "setAdminToken");

    render(
      <MemoryRouter initialEntries={["/login"]}>
        <Routes>
          <Route path="/login" element={<LoginView />} />
          <Route path="/dashboard" element={<div>Dashboard</div>} />
        </Routes>
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByPlaceholderText("eyJhbGciOi..."), {
      target: { value: "my-jwt-token" },
    });
    fireEvent.click(screen.getByRole("button", { name: /sign in/i }));

    expect(setTokenSpy).toHaveBeenCalledWith("my-jwt-token");
    expect(screen.getByText("Dashboard")).toBeInTheDocument();
  });
});
