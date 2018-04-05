# tests for work helper functions
using Base.Test
include("work.jl")


@testset "WorkUnit constructor tests"  begin
    @test_throws ErrorException WorkUnit(-1, 2)
    @test_throws ErrorException WorkUnit(2, 0)
    @test_throws ErrorException WorkUnit(0, 0)
    @test typeof(WorkUnit(2, 3)) == WorkUnit{typeof(2)}
end

@testset "split_work tests" begin
    testwork = WorkUnit(10, 3)
    testwork2 = WorkUnit(555, 4)
    testwork3 = WorkUnit(1, 1)

    @testset "split_work domain tests" begin
        @test_throws DomainError split_work(testwork, -0.5)
        @test_throws DomainError split_work(testwork2, 1.63)
        @test_throws DomainError split_work(testwork3, .5)
    end

    @testset "split_work 50% test" begin
        u1, v1 = split_work(testwork, 0.5)
        u2, v2 = split_work(testwork2, 0.5)
        @test u1.units == 5
        @test v1.units == 5
        @test u2.units == 278
        @test v2.units == 277
    end

    @testset "split_work 0 + ϵ test" begin
        u1, v1 = split_work(testwork, eps(Float64))
        u2, v2 = split_work(testwork2, eps(Float64))
        @test u1.units == 1
        @test v1.units == 9
        @test u2.units == 1
        @test v2.units == 554
    end

    @testset "split_work 1 - ϵ test" begin
        u1, v1 = split_work(testwork, 1 - eps(Float64))
        u2, v2 = split_work(testwork2, 1 - eps(Float64))
        @test u1.units == 9
        @test v1.units == 1
        @test u2.units == 554
        @test v2.units == 1
    end

end
